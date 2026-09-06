"""Telegram 命令、文本和媒体 Update 的入口处理器。"""

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from secrets import token_urlsafe
from typing import Any
from urllib.parse import urlsplit

import httpx
from telegram import (
    Chat,
    ChatMember,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    LinkPreviewOptions,
    Message,
    MessageEntity,
    MessageOrigin,
    MessageOriginChannel,
    MessageOriginChat,
    MessageOriginHiddenUser,
    MessageOriginUser,
    Update,
    User,
)
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    BaseHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)
from telegram.helpers import escape_markdown

from src.bus import message_bus
from src.config import config
from src.forwarding import (
    telegram_forward_task,
    telegram_group_member_task,
    telegram_pin_task,
    telegram_processing_task,
)
from src.log import baselog
from src.mapping_outbox import mapping_outbox, newest_mapping, newest_pending_mapping
from src.media import (
    TELEGRAM_DOWNLOAD_LIMIT,
    TELEGRAM_DOWNLOAD_LIMIT_TEXT,
    MediaFile,
    media_item_budget,
    media_queue_budget,
)
from src.messages import (
    ONEBOT_SEND_FAILED_TEXT,
    ONEBOT_USER_NAME,
    OneBotConnectionError,
    OneBotResultUnknownError,
    SendTarget,
    SendTargetUnavailableError,
    SendTask,
    TelegramGroupMemberEvent,
    TelegramMedia,
    TelegramMessage,
    onebot_user_name,
)
from src.notice import enqueue_bridge_notice, enqueue_telegram_notice
from src.processing import media_processor
from src.qbot import q_gateway
from src.runtime_events import emit_runtime_event
from src.runtime_stats import get_runtime_info
from src.sql import sql

TELEGRAM_VIDEO_LIMIT = TELEGRAM_DOWNLOAD_LIMIT
TELEGRAM_ALBUM_LIMIT = 10
TELEGRAM_ALBUM_BYTES_LIMIT = 100_000_000
DOWNLOAD_CHUNK_SIZE = 256 * 1024
INLINE_AT_TTL = 5 * 60
INLINE_AT_PAGE_SIZE = 50
INLINE_AT_CONTEXT_LIMIT = 256
INLINE_AT_SNAPSHOT_LIMIT = 64
INLINE_AT_MEMBER_LIMIT = 3_000
INLINE_AT_SELECTION_LIMIT = 4096
INLINE_AT_URL_PREFIX = "https://q2tg.invalid/token/"
INLINE_AT_MARKER = "\u2063"

# Telegram delete_messages 单次最多 100 条。
TELEGRAM_DELETE_BATCH = 100
# 群聊管理员身份集合，多个命令共用。
GROUP_ADMIN_STATUSES = {ChatMember.ADMINISTRATOR, ChatMember.OWNER}
# 视为群聊的 chat.type 集合。
GROUP_CHAT_TYPES = {"group", "supergroup"}

# 复用的用户可见文案。
NOT_BOUND_TEXT = "当前群聊尚未绑定 OneBot 群"
GROUP_ONLY_TEXT = "请在 Telegram 群聊中使用此命令"
ONEBOT_ERROR_TEXT = "OneBot 错误，请检查日志"
NO_MAPPING_TEXT = "未找到该消息的跨平台映射"


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """单一真相源的命令定义：命令名、菜单描述和 TGhandlers 方法名。

    get_handlers 据此生成 CommandHandler，BOT_COMMANDS 据此生成菜单，
    命令名不再在注册与菜单两处分别硬编码。
    """

    name: str
    description: str
    handler_attr: str


# 命令注册顺序即菜单展示顺序：功能命令在前，状态查询在后。
COMMAND_SPECS: tuple[CommandSpec, ...] = (
    CommandSpec("start", "查看 Bot 运行状态", "start"),
    CommandSpec("bind", "绑定当前 Telegram 群与 OneBot 群", "bind"),
    CommandSpec("unbind", "解除当前群的 OneBot 群绑定", "unbind"),
    CommandSpec("forward", "查看或设置双向消息转发", "forward"),
    CommandSpec("bot_forward", "查看或设置其他 Bot 消息转发", "bot_forward"),
    CommandSpec("id_show", "查看或设置 OneBot 用户 ID 显示", "id_show"),
    CommandSpec("at", "选择需要 @ 的 OneBot 群成员", "at"),
    CommandSpec("undo", "撤回所回复消息的双侧副本", "undo"),
    CommandSpec("unpin", "取消所回复消息的置顶和精华", "unpin"),
    CommandSpec("status", "查看 Q2TG 运行状态", "get_status"),
)


@dataclass(slots=True)
class InlineAtContext:
    """由群聊命令创建、供 Inline Query 恢复群上下文的短期令牌。"""

    user_id: int
    tg_chat_id: int
    q_group_id: int
    expires_at: float


@dataclass(slots=True)
class InlineAtMemberSnapshot:
    members: list[dict[str, Any]]
    expires_at: float


@dataclass(slots=True)
class InlineAtSelection:
    context_token: str
    user_id: int
    tg_chat_id: int
    q_group_id: int
    q_user_id: int
    expires_at: float


def forward_origin_name(origin: MessageOrigin | None) -> str | None:
    """提取 Telegram 转发来源的可见名称。"""
    if isinstance(origin, MessageOriginUser):
        return origin.sender_user.full_name
    if isinstance(origin, MessageOriginHiddenUser):
        return origin.sender_user_name.strip() or None
    if isinstance(origin, MessageOriginChat):
        return origin.sender_chat.title or (
            f"@{origin.sender_chat.username}" if origin.sender_chat.username else None
        )
    if isinstance(origin, MessageOriginChannel):
        return origin.chat.title or (
            f"@{origin.chat.username}" if origin.chat.username else None
        )
    return None


def is_anonymous_sender(msg: Message) -> bool:
    """匿名管理员以群身份发言：sender_chat 即当前群，其 from_user 是
    GroupAnonymousBot（is_bot 为真），但本质是群成员而非其他 Bot。"""
    sender_chat = getattr(msg, "sender_chat", None)
    return sender_chat is not None and sender_chat.id == msg.chat_id


def telegram_media_kind(msg: Message) -> str:
    """返回真实测试日志使用的粗粒度 Telegram 媒体类型。"""
    if msg.video is not None:
        return "video"
    if getattr(msg, "voice", None) is not None:
        return "record"
    if msg.photo:
        return "image"
    return "file"


def telegram_message_has_spoiler(msg: Message) -> bool:
    """判断消息或媒体是否带 Telegram 遮罩（spoiler）。"""
    if getattr(msg, "has_media_spoiler", False):
        return True
    spoiler_type = getattr(MessageEntity, "SPOILER", "spoiler")
    return any(
        getattr(entity, "type", None) == spoiler_type
        for entities in (
            getattr(msg, "entities", None),
            getattr(msg, "caption_entities", None),
        )
        for entity in (entities or ())
    )


class TGhandlers:
    """Telegram 命令、文本和媒体入口集合。

    Handler 的职责是尽快把 Telegram Update 转换为内部 TelegramMessage 并入队。实际
    OneBot 发送由消息消费者完成，避免 Telegram 更新处理逻辑直接依赖 WebSocket。
    """

    def __init__(self) -> None:
        # Telegram 相册会拆成多个 Update；以 media_group_id 暂存到同一列表。
        self._albums: dict[str, list[Message]] = {}

        # 每个相册只创建一个延迟 flush 任务，后续图片只追加到列表。
        self._album_tasks: dict[str, asyncio.Task[None]] = {}

        # 由 SnowLuma WebSocket 会话注入和关闭，handlers 不拥有该客户端的生命周期。
        self.download_client: httpx.AsyncClient | None = None

        # Inline Query 不包含当前 chat_id；/at 先把绑定群写入短期随机令牌。
        self._inline_at_contexts: dict[str, InlineAtContext] = {}
        self._inline_at_member_snapshots: dict[int, InlineAtMemberSnapshot] = {}
        self._inline_at_selections: dict[str, InlineAtSelection] = {}
        self._inline_at_selection_tokens: dict[tuple[str, int], str] = {}

    @staticmethod
    async def _put_onebot_task(msg: Message, task: SendTask) -> bool:
        try:
            await message_bus.put(task)
        except SendTargetUnavailableError:
            await msg.reply_text(ONEBOT_SEND_FAILED_TEXT)
            return False
        return True

    @staticmethod
    async def _require_group_context(
        update: Update,
    ) -> tuple[Message, Chat, User] | None:
        """取得群聊命令所需的 message/chat/user，非群聊或缺字段时回复并返回 None。"""
        msg = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if msg is None or chat is None or user is None:
            return None
        if chat.type not in GROUP_CHAT_TYPES:
            # 私聊没有可作为桥接目标的群 chat_id，因此拒绝执行。
            await msg.reply_text(GROUP_ONLY_TEXT)
            return None
        return msg, chat, user

    @staticmethod
    async def _is_group_admin(bot: Any, chat_id: int, user_id: int) -> bool:
        """判断用户是否为目标群的管理员或群主。"""
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in GROUP_ADMIN_STATUSES

    @staticmethod
    def _parse_toggle_args(raw_args: list[str] | None) -> list[str] | None:
        """校验 on/off 开关参数；非法时返回 None，合法时返回小写参数列表。"""
        args = [arg.lower() for arg in (raw_args or [])]
        if len(args) > 1 or (args and args[0] not in {"on", "off"}):
            return None
        return args

    async def _handle_simple_toggle(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        usage_command: str,
        admin_error: str,
        getter: Callable[[int], Awaitable[bool | None]],
        setter: Callable[[int, bool], Awaitable[bool]],
        status_text: Callable[[bool], str],
    ) -> None:
        """处理只回复本地状态、不通知对端的 on/off 开关命令。"""
        context_group = await self._require_group_context(update)
        if context_group is None:
            return
        msg, chat, user = context_group
        if not await self._is_group_admin(context.bot, chat.id, user.id):
            await msg.reply_text(admin_error)
            return
        args = self._parse_toggle_args(context.args)
        if args is None:
            await msg.reply_text(f"用法：{usage_command} [on|off]")
            return
        if not args:
            enabled = await getter(chat.id)
            if enabled is None:
                await msg.reply_text(NOT_BOUND_TEXT)
                return
            await msg.reply_text(status_text(enabled))
            return
        enabled = args[0] == "on"
        if not await setter(chat.id, enabled):
            await msg.reply_text(NOT_BOUND_TEXT)
            return
        await msg.reply_text(status_text(enabled))

    @staticmethod
    async def _fetch_group_name(msg: Message, q_group_id: int, action: str) -> str | None:
        """查询 OneBot 群名称；失败时回复错误提示并返回 None。"""
        try:
            group = await q_gateway.get_group_info(q_group_id)
            group_name = group.get("group_name")
            if not isinstance(group_name, str) or not group_name:
                raise TypeError(f"OneBot 群资料缺少 group_name: {group!r}")
        except (
            OneBotConnectionError,
            OneBotResultUnknownError,
            RuntimeError,
            TimeoutError,
            TypeError,
        ):
            baselog.exception("%s时查询 OneBot 群资料失败: %s", action, q_group_id)
            await msg.reply_text(ONEBOT_ERROR_TEXT)
            return None
        return group_name

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """在群聊显示状态，在私聊提供管理员引导或联系方式。"""
        msg = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if msg is None or chat is None or user is None:
            return

        if chat.type in GROUP_CHAT_TYPES:
            await self.get_status(update, context)
            return

        if user.id == config.tgbot_admin:
            await msg.reply_markdown_v2(
                text=(
                    "欢迎使用 Q2TG\\-Python！\n\n"
                    "请先将本 Bot 加入需要桥接的 Telegram 群聊，"
                    "并将 OneBot 机器人加入对应的 OneBot 群聊。\n\n"
                    "然后在 Telegram 群聊中发送：\n"
                    "`/bind <OneBot 群号>`\n"
                    "或在当前私聊中发送：\n"
                    "`/bind <Telegram 聊天 ID> <OneBot 群号>`\n"
                    "`/unbind <Telegram 聊天 ID或OneBot 群号>`\n\n"
                    "Copyright 2026 Azusa\\-mikan"
                )
            )
            return

        admin_user = await context.bot.get_chat(config.tgbot_admin)
        admin_name = escape_markdown(
            admin_user.full_name or str(admin_user.id),
            version=2,
        )
        user_url = (
            f"https://t.me/{admin_user.username}"
            if admin_user.username is not None
            else f"tg://user?id={admin_user.id}"
        )
        await msg.reply_markdown_v2(
            f"你无权使用此机器人，请联系 [{admin_name}]({user_url})"
        )

    async def bind(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """由管理员在目标 Telegram 群或私聊中建立群绑定。"""
        msg = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if msg is None or chat is None or user is None:
            return
        if user.id != config.tgbot_admin:
            await msg.reply_text("只有管理员可以绑定群聊")
            return
        args = context.args or []
        if chat.type in GROUP_CHAT_TYPES:
            if len(args) != 1:
                await msg.reply_text("用法：/bind <OneBot 群号>")
                return
            target_chat = chat
            q_group_arg = args[0]
        elif chat.type == "private":
            if len(args) != 2:
                await msg.reply_text("用法：/bind <Telegram 聊天 ID> <OneBot 群号>")
                return
            try:
                tg_chat_id = int(args[0])
            except ValueError:
                await msg.reply_text("Telegram 聊天 ID 必须是整数")
                return
            try:
                target_chat = await context.bot.get_chat(tg_chat_id)
            except (BadRequest, Forbidden):
                await msg.reply_text("无法访问指定的 Telegram 聊天")
                return
            if target_chat.type not in GROUP_CHAT_TYPES:
                await msg.reply_text("指定的 Telegram 聊天不是群聊")
                return
            q_group_arg = args[1]
        else:
            await msg.reply_text("请在 Telegram 群聊或与 Bot 的私聊中使用此命令")
            return

        try:
            q_group_id = int(q_group_arg)
        except ValueError:
            await msg.reply_text("OneBot 群号必须是整数")
            return
        if not target_chat.title:
            await msg.reply_text("Telegram 群资料缺少标题")
            return

        groups = None
        try:
            groups = await q_gateway.get_group_list()
        except (
            OneBotConnectionError,
            OneBotResultUnknownError,
            RuntimeError,
            TimeoutError,
        ):
            baselog.exception("绑定群聊时查询 OneBot 群列表失败: %s", q_group_id)
            await msg.reply_text(ONEBOT_ERROR_TEXT)
            return
        if not any(group.get("group_id") == q_group_id for group in groups):
            await msg.reply_text("OneBot端未找到该群聊")
            return
        group_name = await self._fetch_group_name(msg, q_group_id, "绑定群聊")
        if group_name is None:
            return

        try:
            # Sql 会校验正整数和一对一冲突，不在 handler 中重复规则。
            await sql.bind_group(q_group_id, target_chat.id)
        except ValueError as error:
            await msg.reply_text(str(error))
            return

        success_text = f"已绑定群 {group_name}"
        enqueue_bridge_notice(
            partial(msg.reply_text, success_text),
            q_gateway,
            q_group_id=q_group_id,
            text=f"已绑定群 {target_chat.title}",
        )
        if chat.type == "private":
            enqueue_telegram_notice(
                partial(
                    context.bot.send_message,
                    chat_id=target_chat.id,
                    text=success_text,
                )
            )

    async def unbind(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """在群聊或私聊中解除指定群的双向绑定。"""
        msg = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if msg is None or chat is None or user is None:
            return
        if user.id != config.tgbot_admin:
            await msg.reply_text("只有管理员可以解除绑定")
            return
        args = context.args or []
        if chat.type in GROUP_CHAT_TYPES:
            if args:
                await msg.reply_text("用法：/unbind")
                return
            tg_chat_id = chat.id
            q_group_id = await sql.get_q_group(tg_chat_id)
            if q_group_id is None:
                await msg.reply_text(NOT_BOUND_TEXT)
                return
            target_chat = chat
        elif chat.type == "private":
            if len(args) != 1:
                await msg.reply_text("用法：/unbind <Telegram 聊天 ID或OneBot 群号>")
                return
            try:
                group_id = int(args[0])
            except ValueError:
                await msg.reply_text("群聊 ID 必须是整数")
                return
            if group_id < 0:
                tg_chat_id = group_id
                q_group_id = await sql.get_q_group(tg_chat_id)
            elif group_id > 0:
                q_group_id = group_id
                tg_chat_id = await sql.get_tg_group(q_group_id)
            else:
                q_group_id = None
                tg_chat_id = None
            if q_group_id is None or tg_chat_id is None:
                await msg.reply_text("未找到该群聊的绑定关系")
                return
            try:
                target_chat = await context.bot.get_chat(tg_chat_id)
            except (BadRequest, Forbidden):
                await msg.reply_text("无法访问绑定的 Telegram 群聊")
                return
            if target_chat.type not in GROUP_CHAT_TYPES or not target_chat.title:
                await msg.reply_text("绑定的 Telegram 聊天不是有效群聊")
                return
        else:
            await msg.reply_text("请在 Telegram 群聊或与 Bot 的私聊中使用此命令")
            return

        group_name = await self._fetch_group_name(msg, q_group_id, "解除绑定")
        if group_name is None:
            return

        await sql.unbind_tg_group(tg_chat_id)

        success_text = f"已解绑群 {group_name}"
        enqueue_bridge_notice(
            partial(msg.reply_text, success_text),
            q_gateway,
            q_group_id=q_group_id,
            text=f"已解绑群 {target_chat.title}",
        )
        if chat.type == "private":
            enqueue_telegram_notice(
                partial(
                    context.bot.send_message,
                    chat_id=target_chat.id,
                    text=success_text,
                )
            )

    async def forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """查询或设置当前绑定的双向消息转发开关。"""
        context_group = await self._require_group_context(update)
        if context_group is None:
            return
        msg, chat, user = context_group
        if not await self._is_group_admin(context.bot, chat.id, user.id):
            await msg.reply_text("只有群聊管理员可以设置转发开关")
            return

        args = self._parse_toggle_args(context.args)
        if args is None:
            await msg.reply_text("用法：/forward [on|off]")
            return
        if not args:
            enabled = await sql.get_tg_forward_enabled(chat.id)
            if enabled is None:
                await msg.reply_text(NOT_BOUND_TEXT)
                return
            q_group_id = await sql.get_q_group(chat.id)
            enqueue_bridge_notice(
                partial(
                    msg.reply_text,
                    f"当前双向消息转发已{'开启' if enabled else '关闭'}",
                ),
                q_gateway,
                q_group_id=q_group_id,
                text=f"当前双向消息转发已{'开启' if enabled else '关闭'}",
            )
            return

        enabled = args[0] == "on"
        q_group_id = await sql.get_q_group(chat.id)
        if q_group_id is None:
            await msg.reply_text(NOT_BOUND_TEXT)
            return
        if not await sql.set_tg_forward_enabled(chat.id, enabled):
            await msg.reply_text(NOT_BOUND_TEXT)
            return
        enqueue_bridge_notice(
            partial(
                msg.reply_text,
                f"双向消息转发已{'开启' if enabled else '关闭'}",
            ),
            q_gateway,
            q_group_id=q_group_id,
            text=f"双向消息转发已{'开启' if enabled else '关闭'}",
        )

    async def id_show(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """查询或设置 OneBot 用户及 @ 对象是否在 Telegram 显示数字 ID。"""
        await self._handle_simple_toggle(
            update,
            context,
            usage_command="/id_show",
            admin_error="只有群聊管理员可以设置 ID 显示",
            getter=sql.get_id_show_enabled,
            setter=sql.set_id_show_enabled,
            status_text=lambda enabled: (
                f"OneBot 用户及 @ 对象 ID 显示已{'开启' if enabled else '关闭'}"
            ),
        )

    async def bot_forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """查询或设置当前 Telegram 群的其他 Bot 消息转发开关。"""
        await self._handle_simple_toggle(
            update,
            context,
            usage_command="/bot_forward",
            admin_error="只有群聊管理员可以设置其他 Bot 消息转发",
            getter=sql.get_bot_forward_enabled,
            setter=sql.set_bot_forward_enabled,
            status_text=lambda enabled: f"其他 Bot 消息转发已{'开启' if enabled else '关闭'}",
        )

    async def at(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """在当前绑定群创建 Inline Mode 所需的短期群上下文。"""
        context_group = await self._require_group_context(update)
        if context_group is None:
            return
        msg, chat, user = context_group

        q_group_id = await sql.get_q_group(chat.id)
        if q_group_id is None:
            await msg.reply_text(NOT_BOUND_TEXT)
            return

        self._purge_inline_at_contexts()
        if len(self._inline_at_contexts) >= INLINE_AT_CONTEXT_LIMIT:
            self._inline_at_contexts.pop(next(iter(self._inline_at_contexts)))
        token = token_urlsafe(18)
        self._inline_at_contexts[token] = InlineAtContext(
            user_id=user.id,
            tg_chat_id=chat.id,
            q_group_id=q_group_id,
            expires_at=time.monotonic() + INLINE_AT_TTL,
        )
        await msg.reply_text(
            "请选择需要 @ 的 OneBot 群成员",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("选择群成员", switch_inline_query_current_chat=f"at {token} ")]]
            ),
        )

    async def inline_at(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """按 /at 创建的上下文搜索 OneBot 群成员并返回 Inline 结果。"""
        inline_query = update.inline_query
        if inline_query is None:
            return
        query_parts = inline_query.query.strip().split(maxsplit=2)
        if len(query_parts) < 2 or query_parts[0].lower() != "at":
            return

        token = query_parts[1]
        search = query_parts[2].strip().casefold() if len(query_parts) == 3 else ""
        self._purge_inline_at_contexts()
        inline_context = self._inline_at_contexts.get(token)
        if inline_context is None or inline_context.user_id != inline_query.from_user.id:
            await inline_query.answer([], cache_time=0, is_personal=True)
            return

        members_snapshot = self._inline_at_member_snapshots.get(inline_context.q_group_id)
        if members_snapshot is None:
            if len(self._inline_at_member_snapshots) >= INLINE_AT_SNAPSHOT_LIMIT:
                self._inline_at_member_snapshots.pop(
                    next(iter(self._inline_at_member_snapshots))
                )
            members = await q_gateway.get_group_member_list(inline_context.q_group_id)
            if len(members) > INLINE_AT_MEMBER_LIMIT:
                baselog.error(
                    "OneBot 群成员列表超过缓存上限: group=%s members=%s limit=%s",
                    inline_context.q_group_id,
                    len(members),
                    INLINE_AT_MEMBER_LIMIT,
                )
                await inline_query.answer([], cache_time=0, is_personal=True)
                return
            members_snapshot = InlineAtMemberSnapshot(
                members=members,
                expires_at=time.monotonic() + INLINE_AT_TTL,
            )
            self._inline_at_member_snapshots[inline_context.q_group_id] = members_snapshot
        id_show = bool(await sql.get_id_show_enabled(inline_context.tg_chat_id))
        members = [
            member
            for member in members_snapshot.members
            if (
                not search
                or any(
                    isinstance(value, str) and search in value.casefold()
                    for value in (member.get("card"), member.get("nickname"))
                )
                or search in str(member.get("user_id", ""))
            )
        ]
        try:
            offset = max(int(inline_query.offset or "0"), 0)
        except ValueError:
            offset = 0
        page = members[offset : offset + INLINE_AT_PAGE_SIZE]
        results = []
        for member in page:
            name = self._inline_member_name(member)
            user_id = member.get("user_id")
            if not isinstance(user_id, int) or isinstance(user_id, bool):
                continue
            selection_token = self._inline_at_selection_token(
                token,
                inline_context,
                user_id,
            )
            title = f"{name}[{user_id}]" if id_show else name
            message_text = f"@{INLINE_AT_MARKER}{title}"
            results.append(
                InlineQueryResultArticle(
                    id=str(user_id),
                    title=title,
                    input_message_content=InputTextMessageContent(
                        message_text,
                        entities=(
                            MessageEntity(
                                type=MessageEntity.TEXT_LINK,
                                offset=1,
                                length=1,
                                url=f"{INLINE_AT_URL_PREFIX}{selection_token}",
                            ),
                        ),
                        link_preview_options=LinkPreviewOptions(is_disabled=True),
                    ),
                )
            )
        next_offset = (
            str(offset + INLINE_AT_PAGE_SIZE)
            if offset + INLINE_AT_PAGE_SIZE < len(members)
            else ""
        )
        await inline_query.answer(
            results,
            cache_time=0,
            is_personal=True,
            next_offset=next_offset,
        )

    @staticmethod
    def _inline_member_name(member: dict[str, Any]) -> str:
        """按群名片、昵称顺序取得 Inline 候选名称。"""
        return onebot_user_name(member) or ONEBOT_USER_NAME

    async def _inline_at_user_id(self, msg: Message, bot_id: int) -> int | None:
        """校验当前 Bot 的 Inline 选择 token 并取得 OneBot 用户 ID。"""
        via_bot = getattr(msg, "via_bot", None)
        text = msg.text
        if (
            via_bot is None
            or via_bot.id != bot_id
            or not text
            or not text.startswith(f"@{INLINE_AT_MARKER}")
        ):
            return None
        entities = msg.entities or ()
        if len(entities) != 1:
            return None
        entity = entities[0]
        if (
            entity.type != MessageEntity.TEXT_LINK
            or entity.offset != 1
            or entity.length != 1
            or not entity.url
            or not entity.url.startswith(INLINE_AT_URL_PREFIX)
        ):
            return None
        parsed_url = urlsplit(entity.url)
        token = parsed_url.path.removeprefix("/token/")
        if (
            parsed_url.scheme != "https"
            or parsed_url.netloc != "q2tg.invalid"
            or parsed_url.path != f"/token/{token}"
            or not token
            or "/" in token
            or parsed_url.query
            or parsed_url.fragment
        ):
            return None

        self._purge_inline_at_contexts()
        selection = self._inline_at_selections.get(token)
        sender = msg.from_user
        if (
            selection is None
            or sender is None
            or selection.user_id != sender.id
            or selection.tg_chat_id != msg.chat_id
            or selection.context_token not in self._inline_at_contexts
            or await sql.get_q_group(msg.chat_id) != selection.q_group_id
        ):
            return None
        return selection.q_user_id

    def _inline_at_selection_token(
        self,
        context_token: str,
        inline_context: InlineAtContext,
        q_user_id: int,
    ) -> str:
        key = (context_token, q_user_id)
        cached_token = self._inline_at_selection_tokens.get(key)
        if cached_token is not None and cached_token in self._inline_at_selections:
            return cached_token
        if len(self._inline_at_selections) >= INLINE_AT_SELECTION_LIMIT:
            oldest_token = next(iter(self._inline_at_selections))
            oldest = self._inline_at_selections.pop(oldest_token)
            self._inline_at_selection_tokens.pop(
                (oldest.context_token, oldest.q_user_id),
                None,
            )
        selection_token = token_urlsafe(24)
        self._inline_at_selections[selection_token] = InlineAtSelection(
            context_token=context_token,
            user_id=inline_context.user_id,
            tg_chat_id=inline_context.tg_chat_id,
            q_group_id=inline_context.q_group_id,
            q_user_id=q_user_id,
            expires_at=inline_context.expires_at,
        )
        self._inline_at_selection_tokens[key] = selection_token
        return selection_token

    def _purge_inline_at_contexts(self) -> None:
        now = time.monotonic()
        for token, inline_context in list(self._inline_at_contexts.items()):
            if inline_context.expires_at <= now:
                self._inline_at_contexts.pop(token, None)
        for group_id, snapshot in list(self._inline_at_member_snapshots.items()):
            if snapshot.expires_at <= now:
                self._inline_at_member_snapshots.pop(group_id, None)
        for token, selection in list(self._inline_at_selections.items()):
            if (
                selection.expires_at <= now
                or selection.context_token not in self._inline_at_contexts
            ):
                self._inline_at_selections.pop(token, None)
                self._inline_at_selection_tokens.pop(
                    (selection.context_token, selection.q_user_id),
                    None,
                )

    @staticmethod
    def _inline_at_entity_log(entity: MessageEntity) -> dict[str, Any]:
        url = entity.url
        url_info: str | None = None
        if url:
            parsed = urlsplit(url)
            fingerprint = hashlib.sha256(url.encode()).hexdigest()[:12]
            url_info = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rsplit('/', 1)[0]}/<sha256:{fingerprint}>"
        return {
            "type": str(entity.type),
            "offset": entity.offset,
            "length": entity.length,
            "url": url_info,
        }

    @staticmethod
    async def _can_forward_sender(msg: Message, bot_id: int | None) -> bool:
        sender = msg.from_user
        # 匿名管理员的 from_user 是 GroupAnonymousBot（is_bot 为真），但它是群成员
        # 以群身份发言，不是其他 Bot，直接放行、不受 bot_forward 开关约束。
        if is_anonymous_sender(msg):
            return True
        if sender is None or not getattr(sender, "is_bot", False):
            return True
        if bot_id is not None and sender.id == bot_id:
            return False
        return bool(await sql.get_bot_forward_enabled(msg.chat_id))

    async def undo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """撤回被回复消息在 Telegram 和 OneBot 两侧的对应消息。"""
        context_group = await self._require_group_context(update)
        if context_group is None:
            return
        msg, chat, user = context_group
        target = msg.reply_to_message
        if target is None:
            await msg.reply_text("请回复需要撤回的消息后使用 /undo")
            return
        pending_before = mapping_outbox.get_q_message(chat.id, target.message_id)
        try:
            mapping = await sql.get_q_message(chat.id, target.message_id)
        except Exception:
            pending = newest_pending_mapping(
                pending_before,
                mapping_outbox.get_q_message(chat.id, target.message_id),
            )
            if pending is None:
                raise
            mapping = pending
        else:
            pending = newest_pending_mapping(
                pending_before,
                mapping_outbox.get_q_message(chat.id, target.message_id),
            )
            mapping = newest_mapping(mapping, pending)
        if mapping is None:
            await msg.reply_text(NO_MAPPING_TEXT)
            return
        is_admin = await self._is_group_admin(context.bot, chat.id, user.id)
        # target 通常是 Bot 发出的转发消息，因此必须使用映射中保存的原始 TG 用户 ID。
        is_owner = mapping.tg_user_id == user.id
        if not is_admin and not is_owner:
            await msg.reply_text("非群聊管理员只能撤回自己的消息")
            return

        # 目标消息由群主发送、机器人仅为管理员时，OneBot 可能返回成功，
        # 但群聊权限仍会阻止实际撤回。
        failures, connected = await self._run_onebot_message_actions(
            mapping.q_message_ids,
            q_gateway.delete_message,
        )
        if not connected:
            await msg.reply_text("OneBot 连接已断开，请稍后重试")
            return
        if failures:
            await msg.reply_text("OneBot 撤回失败，消息可能超过两分钟或机器人权限不足")
            return
        for index in range(0, len(mapping.tg_message_ids), TELEGRAM_DELETE_BATCH):
            await context.bot.delete_messages(
                chat_id=mapping.tg_chat_id,
                message_ids=mapping.tg_message_ids[index : index + TELEGRAM_DELETE_BATCH],
            )
        await msg.delete()
        emit_runtime_event("command.succeeded", "telegram-undo")

    async def unpin(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """取消被回复消息在 Telegram 的置顶和 OneBot 的精华状态。"""
        context_group = await self._require_group_context(update)
        if context_group is None:
            return
        msg, chat, user = context_group
        target = msg.reply_to_message
        if target is None:
            await msg.reply_text("请回复需要取消置顶的消息后使用 /unpin")
            return
        if not await self._is_group_admin(context.bot, chat.id, user.id):
            await msg.reply_text("只有群聊管理员可以取消置顶")
            return
        pending_before = mapping_outbox.get_q_message(chat.id, target.message_id)
        try:
            mapping = await sql.get_q_message(chat.id, target.message_id)
        except Exception:
            pending = newest_pending_mapping(
                pending_before,
                mapping_outbox.get_q_message(chat.id, target.message_id),
            )
            if pending is None:
                raise
            mapping = pending
        else:
            pending = newest_pending_mapping(
                pending_before,
                mapping_outbox.get_q_message(chat.id, target.message_id),
            )
            mapping = newest_mapping(mapping, pending)
        if mapping is None:
            await msg.reply_text(NO_MAPPING_TEXT)
            return

        failures, connected = await self._run_onebot_message_actions(
            mapping.q_message_ids,
            q_gateway.delete_essence_message,
        )
        if not connected:
            await msg.reply_text("OneBot 连接已断开，请稍后重试")
            return
        if failures:
            await msg.reply_text("OneBot 取消精华失败，机器人权限可能不足")
            return
        for message_id in mapping.tg_message_ids:
            await context.bot.unpin_chat_message(
                chat_id=mapping.tg_chat_id,
                message_id=message_id,
            )
        await msg.delete()
        emit_runtime_event("command.succeeded", "telegram-unpin")

    @staticmethod
    async def _run_onebot_message_actions(
        message_ids: tuple[int, ...],
        action: Callable[[int], Awaitable[None]],
    ) -> tuple[int, bool]:
        failures = 0
        for message_id in message_ids:
            try:
                await action(message_id)
            except (OneBotConnectionError, OneBotResultUnknownError):
                return failures, False
            except RuntimeError:
                failures += 1
        return failures, True

    async def get_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """显示进程内存、队列长度和媒体转换平均耗时。"""
        msg = update.effective_message
        if msg is None:
            return
        data = get_runtime_info()
        queues = data.queues
        averages = data.conversion_averages

        def average_text(value: float | None) -> str:
            return "暂无数据" if value is None else f"{value:.2f} 秒"

        reply_text = (
            "Q2TG 状态\n\n"
            f"RSS：{data.rss}\n\n"
            "消息队列\n"
            f"OneBot 消息：{queues.onebot_messages}\n"
            f"OneBot 事件：{queues.onebot_events}\n"
            f"OneBot 系统通知：{queues.onebot_system}\n"
            f"Telegram 消息：{queues.telegram_messages}\n"
            f"Telegram 事件：{queues.telegram_events}\n"
            f"Telegram 系统通知：{queues.telegram_system}\n"
            f"重试：{queues.retry}\n"
            f"媒体处理：{queues.media_processing}\n\n"
            "平均转换耗时（最近 30 次）\n"
            f"语音：{average_text(averages.voice)}\n"
            f"视频：{average_text(averages.video)}\n"
            f"静态贴纸：{average_text(averages.sticker_static)}\n"
            f"TGS 贴纸：{average_text(averages.sticker_tgs)}\n"
            f"视频贴纸：{average_text(averages.sticker_video)}"
        )
        await msg.reply_text(reply_text)

    async def receive_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """把 Telegram 群中的文本转换为 TelegramMessage。"""
        await self._receive_text(update, context, bot_forward_required=False)

    async def _receive_text(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        bot_forward_required: bool,
    ) -> None:
        msg = update.effective_message
        if msg is None:
            return
        if not await sql.get_tg_forward_enabled(msg.chat_id):
            return
        if not await self._can_forward_sender(msg, context.bot.id):
            return
        if telegram_message_has_spoiler(msg):
            return
        user_id = msg.from_user.id if msg.from_user is not None else 0
        sender_name = msg.from_user.full_name if msg.from_user is not None else f"Telegram用户 {user_id}"
        at_user_id = await self._inline_at_user_id(msg, context.bot.id)
        via_bot = getattr(msg, "via_bot", None)
        if (
            at_user_id is None
            and via_bot is not None
            and via_bot.id == context.bot.id
            and msg.text
            and msg.text.startswith("@")
        ):
            baselog.error(
                "当前 Bot 的 Inline @ 结果无法恢复 OneBot 用户 ID，已阻止文本降级: "
                "chat=%s message=%s user=%s via_bot=%s entities=%r",
                msg.chat_id,
                msg.message_id,
                user_id,
                via_bot.id,
                [self._inline_at_entity_log(entity) for entity in (msg.entities or ())],
            )
            return
        message = TelegramMessage(
            # 纯文本只有一个 Telegram message_id，仍使用元组统一映射结构。
            message_ids=(msg.message_id,),
            group_id=msg.chat_id,
            user_id=user_id,
            sender_name=sender_name,
            text=None if at_user_id is not None else msg.text,
            at_user_id=at_user_id,
            bot_forward_required=(
                bot_forward_required
                or bool(
                    msg.from_user is not None
                    and getattr(msg.from_user, "is_bot", False)
                    and not is_anonymous_sender(msg)
                )
            ),
            forwarded_from=forward_origin_name(getattr(msg, "forward_origin", None)),
            reply_message_id=(
                msg.reply_to_message.message_id if msg.reply_to_message is not None else None
            ),
        )
        await self._put_onebot_task(
            msg,
            telegram_forward_task(message, q_gateway, context.bot),
        )

    async def receive_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """在其他 Bot 消息转发开启时转发未由本 Bot 处理的命令。"""
        msg = update.effective_message
        if msg is None or not await sql.get_bot_forward_enabled(msg.chat_id):
            return
        await self._receive_text(update, context, bot_forward_required=True)

    async def receive_pinned_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """把 Telegram 用户置顶的消息交给 OneBot 精华事件队列。"""
        msg = update.effective_message
        if msg is None or msg.pinned_message is None:
            return
        if msg.from_user is not None and msg.from_user.id == context.bot.id:
            # OneBot 精华同步触发的 Telegram 服务消息不能再次回传。
            return
        if not await sql.get_tg_forward_enabled(msg.chat_id):
            return
        await self._put_onebot_task(
            msg,
            telegram_pin_task(msg.chat_id, msg.pinned_message.message_id, q_gateway),
        )

    async def receive_group_member(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """把 Telegram 群成员加入或退出服务消息交给 OneBot 事件队列。"""
        msg = update.effective_message
        if msg is None or not await sql.get_tg_forward_enabled(msg.chat_id):
            return
        if msg.new_chat_members:
            members = msg.new_chat_members
            joined = True
        elif msg.left_chat_member is not None:
            members = (msg.left_chat_member,)
            joined = False
        else:
            return
        await self._put_onebot_task(
            msg,
            telegram_group_member_task(
                TelegramGroupMemberEvent(
                    group_id=msg.chat_id,
                    member_names=tuple(member.full_name for member in members),
                    joined=joined,
                ),
                q_gateway,
            ),
        )

    async def receive_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理单个图片、视频、语音或文件，或按 media_group_id 收集媒体组。"""
        msg = update.effective_message
        if msg is None:
            return
        if msg.media_group_id is None:
            try:
                await self._enqueue_media_while_connected([msg], context.bot.id)
            except ValueError as error:
                await msg.reply_text(str(error))
                emit_runtime_event(
                    "inbound.rejected",
                    f"telegram-media:{telegram_media_kind(msg)}",
                    error=error,
                )
            return

        album = self._albums.setdefault(msg.media_group_id, [])
        album.append(msg)
        if msg.media_group_id not in self._album_tasks:
            # PTB Application 创建任务后会跟踪其异常和关闭行为。
            task = context.application.create_task(
                self._flush_album(msg.media_group_id),
                update=update,
            )
            self._album_tasks[msg.media_group_id] = task

    async def _flush_album(self, media_group_id: str) -> None:
        """等待短聚合窗口后，把同一相册作为一条内部消息处理。"""
        try:
            # Telegram 没有明确的“相册结束”事件，只能通过短暂静默判断收集完成。
            await asyncio.sleep(0.75)
            messages = self._albums.pop(media_group_id, [])
            if messages:
                try:
                    await self._enqueue_media_while_connected(
                        messages,
                        messages[0].get_bot().id,
                    )
                except ValueError as error:
                    await messages[0].reply_text(str(error))
                    emit_runtime_event(
                        "inbound.rejected",
                        "telegram-media-group",
                        error=error,
                    )
        finally:
            # 取消可能发生在聚合窗口内，此时还没有 pop 相册消息。
            self._albums.pop(media_group_id, None)
            self._album_tasks.pop(media_group_id, None)

    async def _enqueue_media_while_connected(
        self,
        messages: list[Message],
        bot_id: int | None,
    ) -> None:
        try:
            await message_bus.run_while_target_available(
                SendTarget.ONEBOT,
                self._enqueue_media(messages, bot_id),
            )
        except SendTargetUnavailableError as error:
            raise ValueError(ONEBOT_SEND_FAILED_TEXT) from error

    async def _enqueue_media(self, messages: list[Message], bot_id: int | None = None) -> None:
        """下载一组 Telegram 媒体，取得资源预算后放入消息队列。

         Telegram 的 Message.photo 是同一张照片的多个尺寸，不是多张照片；这里
        选择最后一个最大尺寸。video、voice、audio 和 document 分别映射为
        OneBot 的 video、record、file 和 file。函数成功入队后，MediaFile 所有权
        交给转发任务；此前的异常或取消路径由本函数清理。
        """
        # Update 到达次序不一定稳定，按 message_id 恢复用户发送的相册顺序。
        messages.sort(key=lambda message: message.message_id)
        if len(messages) > TELEGRAM_ALBUM_LIMIT:
            raise ValueError("Telegram 媒体组超过 10 项上限")

        first = messages[0]
        if not await sql.get_tg_forward_enabled(first.chat_id):
            return
        if not await self._can_forward_sender(first, bot_id):
            return
        if any(telegram_message_has_spoiler(message) for message in messages):
            return
        user_id = first.from_user.id if first.from_user is not None else 0
        sender_name = first.from_user.full_name if first.from_user is not None else f"Telegram用户 {user_id}"
        sources = []
        for message in messages:
            if (sticker := getattr(message, "sticker", None)) is not None:
                if sticker.is_animated:
                    sources.append(
                        (
                            "image",
                            sticker,
                            TELEGRAM_DOWNLOAD_LIMIT,
                            "sticker_tgs",
                        )
                    )
                elif sticker.is_video:
                    sources.append(
                        (
                            "image",
                            sticker,
                            TELEGRAM_DOWNLOAD_LIMIT,
                            "sticker_video",
                        )
                    )
                else:
                    sources.append(
                        (
                            "image",
                            sticker,
                            TELEGRAM_DOWNLOAD_LIMIT,
                            "sticker_static",
                        )
                    )
            elif message.video is not None:
                sources.append(("video", message.video, TELEGRAM_DOWNLOAD_LIMIT, "video"))
            elif (voice := getattr(message, "voice", None)) is not None:
                sources.append(("record", voice, TELEGRAM_DOWNLOAD_LIMIT, "none"))
            elif message.photo:
                sources.append(("image", message.photo[-1], TELEGRAM_DOWNLOAD_LIMIT, "none"))
            elif (audio := getattr(message, "audio", None)) is not None:
                sources.append(("file", audio, TELEGRAM_DOWNLOAD_LIMIT, "none"))
            elif (document := message.document) is not None:
                sources.append(("file", document, TELEGRAM_DOWNLOAD_LIMIT, "none"))

        work_id = f"telegram-to-onebot:{first.chat_id}:{tuple(message.message_id for message in messages)}"
        for kind, _, _, processing in sources:
            capability = {
                "sticker_static": "telegram.sticker.static.input",
                "sticker_video": "telegram.sticker.video.input",
                "sticker_tgs": "telegram.sticker.tgs.input",
                "video": "telegram.media.video",
            }.get(processing, f"telegram.media.{kind}")
            emit_runtime_event("capability.succeeded", capability, work_id=work_id)
        if len(sources) == TELEGRAM_ALBUM_LIMIT:
            emit_runtime_event(
                "capability.succeeded",
                "telegram.media-group.limit",
                work_id=work_id,
            )

        declared_size = sum(source.file_size or limit for _, source, limit, _ in sources)
        if any(
            source.file_size is not None and source.file_size > limit
            for _, source, limit, _ in sources
        ):
            raise ValueError(
                f"Telegram 媒体超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT}，无法转发"
            )
        if declared_size > TELEGRAM_ALBUM_BYTES_LIMIT:
            raise ValueError("Telegram 媒体组超过 100 MB 上限")

        if self.download_client is None:
            raise RuntimeError("Telegram 媒体下载客户端尚未启动")

        media: list[TelegramMedia] = []
        # 先按最坏情况占用队列预算，再开始网络下载。否则多个并发相册都可能先
        # 下载完几十 MB，最后才发现预算不足，预算就失去了限制临时存储的意义。
        reserved_bytes = sum(limit for _, _, limit, _ in sources)
        await media_queue_budget.acquire(reserved_bytes)
        reserved_items = len(sources)
        item_budget_acquired = False
        try:
            # 为 OneBot -> Telegram 的消费者下载保留一个临时文件槽位，避免死锁。
            await media_item_budget.acquire(len(sources), reserve=1)
            item_budget_acquired = True
            for kind, source, size_limit, processing in sources:
                # get_file 获取至少一小时有效的下载 URL 和更准确的文件元数据。
                file = await source.get_file()
                if file.file_size is not None and file.file_size > size_limit:
                    raise ValueError(
                        f"Telegram 媒体超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT}，无法转发"
                    )
                if not file.file_path:
                    raise RuntimeError("Telegram 媒体缺少下载地址")

                source_filename = getattr(source, "file_name", None)
                filename = source_filename or Path(file.file_path).name
                if not filename:
                    filename = {
                        "image": "image.jpg",
                        "record": "voice.ogg",
                        "video": "video.mp4",
                    }.get(kind, "file")
                media_type = getattr(source, "mime_type", None)
                if not media_type:
                    media_type = {
                        "image": "image/jpeg",
                        "record": "audio/ogg",
                        "video": "video/mp4",
                    }.get(kind, "application/octet-stream")
                # file_size 来自 get_file，用于决定内存分档；缺失时沿用默认阈值。
                content = MediaFile.create_reserved(
                    filename=filename,
                    media_type=media_type,
                    expected_size=file.file_size,
                )
                # create_reserved 消耗的是上面批量取得的名额。每成功创建一个，
                # reserved_items 就减少一个，异常清理时只归还尚未使用的名额。
                reserved_items -= 1
                try:
                    try:
                        async with self.download_client.stream("GET", file.file_path) as response:
                            response.raise_for_status()
                            # 先用响应头提前拒绝，再在读取 chunk 时核对实际总大小。
                            content_length = response.headers.get("content-length")
                            if content_length is not None and int(content_length) > size_limit:
                                raise ValueError(
                                    f"Telegram 媒体超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT}，无法转发"
                                )
                            async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_SIZE):
                                if content.size + len(chunk) > size_limit:
                                    raise ValueError(
                                        f"Telegram 媒体超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT}，无法转发"
                                    )
                                content.write(chunk)
                    except httpx.HTTPError:
                        # Telegram 文件 URL 包含 Bot Token，不能让 HTTPX 异常把 URL
                        # 原样带进 PTB 的 traceback 日志。
                        raise RuntimeError("Telegram 媒体下载失败") from None
                    content.rewind()
                    # 下载完成后游标位于文件末尾，入队前重置以供后续读取。
                    media.append(
                        TelegramMedia(
                            kind=kind,
                            content=content,
                            processing=processing,
                        )
                    )
                except BaseException:
                    content.close()
                    raise

            total_size = sum(attachment.content.size for attachment in media)
            if total_size > TELEGRAM_ALBUM_BYTES_LIMIT:
                raise ValueError("Telegram 媒体组超过 100 MB 上限")
            needs_processing = any(
                attachment.processing != "none" for attachment in media
            )
            if not needs_processing:
                # 图片大小不会再改变，可以立即归还下载前的保守预留。
                await media_queue_budget.release(reserved_bytes - total_size)
                reserved_bytes = total_size
            message = TelegramMessage(
                message_ids=tuple(message.message_id for message in messages),
                group_id=first.chat_id,
                user_id=user_id,
                sender_name=sender_name,
                text=next((message.caption for message in messages if message.caption), None),
                bot_forward_required=bool(
                    first.from_user is not None
                    and getattr(first.from_user, "is_bot", False)
                    and not is_anonymous_sender(first)
                ),
                forwarded_from=next(
                    (
                        name
                        for message in messages
                        if (
                            name := forward_origin_name(
                                getattr(message, "forward_origin", None)
                            )
                        )
                        is not None
                    ),
                    None,
                ),
                reply_message_id=next(
                    (
                        message.reply_to_message.message_id
                        for message in messages
                        if message.reply_to_message is not None
                    ),
                    None,
                ),
                # 媒体组 caption 通常只附在其中一项，取第一条非空文本。
                media=tuple(media),
                # 视频转码可能改变大小，预处理完成前保留最坏情况预算。
                queue_bytes=reserved_bytes,
            )
            if needs_processing:
                task = telegram_processing_task(message, q_gateway, first.get_bot())
                if not media_processor.submit(task):
                    raise ValueError("Telegram 媒体处理队列已满，请稍后重试")
            else:
                try:
                    await message_bus.put(
                        telegram_forward_task(message, q_gateway, first.get_bot())
                    )
                except SendTargetUnavailableError as error:
                    raise ValueError(ONEBOT_SEND_FAILED_TEXT) from error
        except BaseException:
            # BaseException 包含任务取消；关停时取消相册任务也必须关闭临时文件，
            # 并归还已经取得但尚未使用的两类预算。
            for attachment in media:
                attachment.content.close()
            if item_budget_acquired and reserved_items:
                media_item_budget.release(reserved_items)
            if reserved_bytes:
                await media_queue_budget.release(reserved_bytes)
            raise

        if not media:
            return

    def get_handlers(self) -> list[BaseHandler]:
        """显式创建全部 PTB handlers，注册顺序与匹配优先级一目了然。"""
        media_filter = filters.ChatType.GROUPS & (
            filters.PHOTO
            | filters.VIDEO
            | filters.VOICE
            | filters.AUDIO
            | filters.Document.ALL
            | filters.Sticker.ALL
        )
        command_handlers: list[BaseHandler] = [
            CommandHandler(spec.name, getattr(self, spec.handler_attr))
            for spec in COMMAND_SPECS
        ]
        return [
            *command_handlers,
            InlineQueryHandler(self.inline_at, pattern=r"^at(?:\s|$)"),
            MessageHandler(
                filters.ChatType.GROUPS & filters.TEXT & filters.COMMAND,
                self.receive_command,
            ),
            MessageHandler(
                filters.ChatType.GROUPS & filters.StatusUpdate.PINNED_MESSAGE,
                self.receive_pinned_message,
            ),
            MessageHandler(
                filters.ChatType.GROUPS
                & (
                    filters.StatusUpdate.NEW_CHAT_MEMBERS
                    | filters.StatusUpdate.LEFT_CHAT_MEMBER
                ),
                self.receive_group_member,
            ),
            MessageHandler(
                filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
                self.receive_message,
            ),
            MessageHandler(media_filter, self.receive_media),
        ]

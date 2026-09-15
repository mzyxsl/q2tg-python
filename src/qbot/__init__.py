"""OneBot WebSocket 网关、action 响应匹配和群消息事件入口。"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import httpx
from fastapi import WebSocket
from telegram.ext import ExtBot

from src.bus import message_bus
from src.forwarding import (
    abandon_onebot_forward,
    begin_onebot_forward,
    consume_suppressed_onebot_recall,
    onebot_essence_task,
    onebot_forward_task,
    onebot_group_ban_task,
    onebot_group_member_task,
    onebot_poke_task,
    onebot_recall_task,
    request_onebot_recall,
)
from src.log import qlog
from src.messages import (
    ONEBOT_USER_NAME,
    OneBotConnectionError,
    OneBotEssenceEvent,
    OneBotGroupBanEvent,
    OneBotGroupMemberEvent,
    OneBotMessage,
    OneBotPokeEvent,
    OneBotResultUnknownError,
)
from src.notice import enqueue_onebot_notice
from src.runtime_events import emit_runtime_event


class QGateway:
    """管理当前 OneBot WebSocket 及 action 的请求响应关联。

    OneBot 事件和 action 响应共用一条 WebSocket。发送 action 时创建 Future 并以
    echo 为键保存；API 接收循环拿到响应后调用 resolve_response 完成 Future，
    发送协程因此可以像普通请求一样等待结果、超时或断线错误。
    """

    def __init__(self) -> None:
        self._websocket: WebSocket | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_tasks: dict[str, asyncio.Task[Any]] = {}

    def bind(self, websocket: WebSocket) -> None:
        """记录当前唯一活动的 SnowLuma WebSocket。"""
        self._websocket = websocket

    def unbind(self, websocket: WebSocket) -> None:
        """按对象身份解绑，并让所有正在等待的 action 立即失败。"""
        if self._websocket is not websocket:
            # 旧连接延迟执行 finally 时，不能错误清除后来建立的新连接。
            return

        self._websocket = None
        for task in self._pending_tasks.values():
            task.cancel()

    async def wait_pending(self) -> None:
        """等待断线时已唤醒的 action 调用离开 pending 集合。"""
        while self._pending:
            await asyncio.sleep(0)

    def resolve_response(self, data: dict[str, Any]) -> bool:
        """若 JSON 是已知 echo 的响应，则唤醒对应发送协程并返回 True。"""
        echo = data.get("echo")
        if not isinstance(echo, str):
            return False

        future = self._pending.get(echo)
        if future is None or future.done():
            # 未知 echo 可能是普通事件或已经超时的旧响应，交还给调用方处理。
            return False

        future.set_result(data)
        return True

    async def send_group_message(
        self,
        group_id: int,
        message: list[dict[str, Any]],
    ) -> int:
        """调用 OneBot send_group_msg，并返回平台生成的消息 ID。

        每个请求使用随机 echo，最多等待 10 秒。finally 无条件删除 Future，避免
        成功、失败、超时或取消后在 _pending 中留下请求状态。
        """
        response = await self._call_action(
            "send_group_msg",
            {
                "group_id": group_id,
                "message": message,
            },
        )

        data = response.get("data")
        if not isinstance(data, dict):
            raise TypeError(f"OneBot 消息响应缺少 data: {response!r}")

        message_id = data.get("message_id")
        if not isinstance(message_id, int):
            raise TypeError(f"OneBot 消息响应缺少 message_id: {response!r}")

        return message_id

    async def delete_message(self, message_id: int) -> None:
        """调用 OneBot delete_msg 撤回指定消息。"""
        await self._call_action("delete_msg", {"message_id": message_id})

    async def set_essence_message(self, message_id: int) -> None:
        """将指定 OneBot 消息设为精华消息。"""
        await self._call_action("set_essence_msg", {"message_id": message_id})

    async def delete_essence_message(self, message_id: int) -> None:
        """将指定 OneBot 消息移出精华消息。"""
        await self._call_action("delete_essence_msg", {"message_id": message_id})

    async def get_group_member_info(
        self,
        group_id: int,
        user_id: int,
        *,
        no_cache: bool = False,
    ) -> dict[str, Any]:
        """查询群成员资料；no_cache 为 True 时要求 OneBot 跳过缓存。"""
        params: dict[str, Any] = {
            "group_id": group_id,
            "user_id": user_id,
        }
        if no_cache:
            params["no_cache"] = True
        response = await self._call_action(
            "get_group_member_info",
            params,
        )
        data = response.get("data")
        if not isinstance(data, dict):
            raise TypeError(f"OneBot 群成员响应缺少 data: {response!r}")
        return data

    async def get_group_member_list(self, group_id: int) -> list[dict[str, Any]]:
        """跳过缓存查询指定 OneBot 群的全部成员。"""
        response = await self._call_action(
            "get_group_member_list",
            {"group_id": group_id, "no_cache": True},
        )
        data = response.get("data")
        if not isinstance(data, list) or not all(
            isinstance(member, dict) for member in data
        ):
            raise TypeError(f"OneBot 群成员列表响应缺少 data: {response!r}")
        return data

    async def get_stranger_info(self, user_id: int) -> dict[str, Any]:
        """查询指定 OneBot 用户的陌生人资料。"""
        response = await self._call_action(
            "get_stranger_info",
            {"user_id": user_id},
        )
        data = response.get("data")
        if not isinstance(data, dict):
            raise TypeError(f"OneBot 陌生人资料响应缺少 data: {response!r}")
        return data

    async def get_group_info(self, group_id: int) -> dict[str, Any]:
        """跳过缓存查询指定 OneBot 群的资料。"""
        response = await self._call_action(
            "get_group_info",
            {"group_id": group_id, "no_cache": True},
        )
        data = response.get("data")
        if not isinstance(data, dict):
            raise TypeError(f"OneBot 群资料响应缺少 data: {response!r}")
        return data

    async def get_group_list(self) -> list[dict[str, Any]]:
        """跳过缓存查询 OneBot 机器人当前加入的群聊。"""
        response = await self._call_action("get_group_list", {"no_cache": True})
        data = response.get("data")
        if not isinstance(data, list) or not all(
            isinstance(group, dict) for group in data
        ):
            raise TypeError(f"OneBot 群列表响应缺少 data: {response!r}")
        return data

    async def get_forward_messages(self, forward_id: str) -> list[dict[str, Any]]:
        """调用 get_forward_msg，并返回经过外壳校验的合并转发消息。"""
        response = await self._call_action("get_forward_msg", {"id": forward_id})
        data = response.get("data")
        messages = data.get("messages") if isinstance(data, dict) else None
        if not isinstance(messages, list) or not all(
            isinstance(message, dict) for message in messages
        ):
            raise TypeError(f"OneBot 合并转发响应缺少 messages: {response!r}")
        return messages

    async def _call_action(
        self,
        action: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """发送带唯一 echo 的 OneBot action，并校验通用响应外壳。"""
        websocket = self._websocket
        if websocket is None:
            raise OneBotConnectionError("OneBot WebSocket 尚未连接")

        echo = uuid4().hex
        # Future 不执行工作，只是让发送协程等待接收循环稍后填入响应。
        future = asyncio.get_running_loop().create_future()
        action_task = asyncio.current_task()
        if action_task is None:
            raise RuntimeError("OneBot action 缺少当前异步任务")
        self._pending[echo] = future
        self._pending_tasks[echo] = action_task
        try:
            try:
                await websocket.send_json(
                    {
                        "action": action,
                        "params": params,
                        "echo": echo,
                    }
                )
                response = await asyncio.wait_for(future, timeout=10)
            except TimeoutError:
                raise OneBotResultUnknownError("OneBot action 响应超时，执行结果未知") from None
            except asyncio.CancelledError:
                if self._websocket is not websocket:
                    if future.done() and not future.cancelled():
                        response = future.result()
                    else:
                        raise OneBotResultUnknownError(
                            "OneBot WebSocket 已断开，action 执行结果未知"
                        ) from None
                else:
                    raise
            except Exception as error:
                raise OneBotResultUnknownError(
                    "OneBot WebSocket 发送失败，action 执行结果未知"
                ) from error
        finally:
            self._pending.pop(echo, None)
            self._pending_tasks.pop(echo, None)
            if not future.done():
                future.cancel()

        # WebSocket 传输成功不代表 action 成功，必须检查 OneBot 响应外壳。
        if response.get("status") != "ok" or response.get("retcode") != 0:
            raise RuntimeError(f"OneBot action {action} 执行失败: {response!r}")
        return response


# API 和消息消费者共享同一个网关实例；连接重建时只替换内部 WebSocket。
q_gateway = QGateway()


def _onebot_int(value: Any) -> int | None:
    """读取 OneBot number 字段，同时拒绝 Python 中属于 int 子类的 bool。"""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _nonempty_string(value: Any) -> str | None:
    """返回去除首尾空白后的非空字符串。"""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _sender_name(data: dict[Any, Any], user_id: int) -> tuple[str, bool]:
    """按 OneBot 群消息语义选择名称，并标记是否使用了带 ID 的兜底值。"""
    anonymous = data.get("anonymous")
    anonymous_name = (
        _nonempty_string(anonymous.get("name"))
        if isinstance(anonymous, dict)
        else None
    )
    if data.get("sub_type") == "anonymous" or anonymous_name is not None:
        if anonymous_name is not None:
            return anonymous_name, False
        return "匿名用户", False

    sender = data.get("sender")
    if isinstance(sender, dict):
        # 群名片是群内展示名；为空时才使用账号昵称。sender 字段按标准是
        # “尽最大努力提供”，因此每个字段都必须独立校验。
        card = _nonempty_string(sender.get("card"))
        if card is not None:
            return card, False
        nickname = _nonempty_string(sender.get("nickname"))
        if nickname is not None:
            return nickname, False
    return f"{ONEBOT_USER_NAME}[{user_id}]", True


def _normalize_segments(value: Any) -> list[dict[str, Any]] | None:
    """校验 OneBot 数组消息格式，并把 null data 规范化为空对象。"""
    if not isinstance(value, list):
        return None

    segments: list[dict[str, Any]] = []
    for segment in value:
        if not isinstance(segment, dict):
            return None
        segment_type = _nonempty_string(segment.get("type"))
        segment_data = segment.get("data")
        if segment_type is None or segment_data is not None and not isinstance(segment_data, dict):
            return None
        segments.append(
            {
                "type": segment_type,
                "data": segment_data if isinstance(segment_data, dict) else {},
            }
        )
    return segments


async def _receive_group_ban_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    """校验群禁言事件并投递到 Telegram 事件队列。"""
    group_id = _onebot_int(data.get("group_id"))
    operator_id = _onebot_int(data.get("operator_id"))
    user_id = _onebot_int(data.get("user_id"))
    duration = _onebot_int(data.get("duration"))
    sub_type = data.get("sub_type")
    if (
        group_id is None
        or operator_id is None
        or user_id is None
        or duration is None
        or duration < 0
        or sub_type not in {"ban", "lift_ban"}
    ):
        qlog.warning("丢弃字段不规范的 OneBot 群禁言事件")
        return
    accepted = message_bus.put_nowait(
        onebot_group_ban_task(
            OneBotGroupBanEvent(
                group_id=group_id,
                operator_id=operator_id,
                user_id=user_id,
                duration=duration,
                lifted=sub_type == "lift_ban",
            ),
            bot,
            q_gateway,
        )
    )
    if not accepted:
        qlog.error("事件队列已满，丢弃 OneBot 群禁言事件: %s", user_id)
        enqueue_onebot_notice(
            q_gateway,
            q_group_id=group_id,
            text="群禁言事件发送到 Telegram 失败：事件队列已满。",
        )


async def _receive_essence_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    """校验精华消息事件并投递到 Telegram 事件队列。"""
    group_id = _onebot_int(data.get("group_id"))
    message_id = _onebot_int(data.get("message_id"))
    sub_type = data.get("sub_type")
    if group_id is None or message_id is None or sub_type not in {"add", "delete"}:
        qlog.warning("丢弃字段不规范的 OneBot 精华消息事件")
        return
    accepted = message_bus.put_nowait(
        onebot_essence_task(
            OneBotEssenceEvent(
                group_id=group_id,
                message_id=message_id,
                added=sub_type == "add",
            ),
            bot,
        )
    )
    if not accepted:
        qlog.error("事件队列已满，丢弃 OneBot 精华消息事件: %s", message_id)
        enqueue_onebot_notice(
            q_gateway,
            q_group_id=group_id,
            text="精华消息状态同步到 Telegram 失败：事件队列已满。",
        )


async def _receive_group_recall_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    """校验群消息撤回事件并投递或登记为在途撤回。"""
    group_id = _onebot_int(data.get("group_id"))
    message_id = _onebot_int(data.get("message_id"))
    if group_id is None or message_id is None:
        qlog.warning("丢弃字段不规范的 OneBot 群消息撤回事件")
        return
    if consume_suppressed_onebot_recall(group_id, message_id):
        qlog.debug(
            "忽略桥接自身触发的 OneBot 撤回回调: group=%s message=%s",
            group_id,
            message_id,
        )
        return
    if request_onebot_recall(group_id, message_id):
        return
    accepted = message_bus.put_nowait(
        onebot_recall_task(group_id, message_id, bot)
    )
    if not accepted:
        qlog.error("事件队列已满，丢弃 OneBot 群消息撤回事件: %s", message_id)
        enqueue_onebot_notice(
            q_gateway,
            q_group_id=group_id,
            text="Telegram 对应消息撤回失败：事件队列已满，请稍后重试。",
        )


async def _receive_group_member_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
    *,
    joined: bool,
) -> None:
    """校验群成员加入或退出事件并投递到 Telegram 事件队列。"""
    group_id = _onebot_int(data.get("group_id"))
    user_id = _onebot_int(data.get("user_id"))
    if group_id is None or user_id is None:
        qlog.warning("丢弃字段不规范的 OneBot 群成员变动事件")
        return
    accepted = message_bus.put_nowait(
        onebot_group_member_task(
            OneBotGroupMemberEvent(
                group_id=group_id,
                user_id=user_id,
                joined=joined,
            ),
            bot,
            q_gateway,
        )
    )
    if not accepted:
        qlog.error("事件队列已满，丢弃 OneBot 群成员变动事件: %s", user_id)
        enqueue_onebot_notice(
            q_gateway,
            q_group_id=group_id,
            text="群成员变动事件发送到 Telegram 失败：事件队列已满。",
        )


async def _receive_group_increase_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    await _receive_group_member_notice(data, bot, joined=True)


async def _receive_group_decrease_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    await _receive_group_member_notice(data, bot, joined=False)


async def _receive_notify_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    """校验 notify.poke，并投递到 Telegram 事件队列。"""
    if data.get("sub_type") != "poke":
        return
    group_id = _onebot_int(data.get("group_id"))
    user_id = _onebot_int(data.get("user_id"))
    target_id = _onebot_int(data.get("target_id"))
    action = _nonempty_string(data.get("action"))
    suffix = data.get("suffix")
    if (
        group_id is None
        or user_id is None
        or target_id is None
        or action is None
        or not isinstance(suffix, str)
    ):
        qlog.warning("丢弃字段不规范的 OneBot 群戳一戳事件")
        return
    accepted = message_bus.put_nowait(
        onebot_poke_task(
            OneBotPokeEvent(
                group_id=group_id,
                user_id=user_id,
                target_id=target_id,
                action=action,
                suffix=suffix,
            ),
            bot,
            q_gateway,
        )
    )
    if not accepted:
        qlog.error("事件队列已满，丢弃 OneBot 群戳一戳事件: %s", user_id)
        enqueue_onebot_notice(
            q_gateway,
            q_group_id=group_id,
            text="群戳一戳事件发送到 Telegram 失败：事件队列已满。",
        )


NoticeHandler = Callable[[dict[Any, Any], ExtBot[None]], Awaitable[None]]
NOTICE_HANDLERS: dict[str, NoticeHandler] = {
    "essence": _receive_essence_notice,
    "group_ban": _receive_group_ban_notice,
    "group_decrease": _receive_group_decrease_notice,
    "group_increase": _receive_group_increase_notice,
    "group_recall": _receive_group_recall_notice,
    "notify": _receive_notify_notice,
}


async def _receive_onebot_notice(
    data: dict[Any, Any],
    bot: ExtBot[None],
) -> None:
    """按 notice_type 把 OneBot 通知分发给对应事件处理器。"""
    notice_type = data.get("notice_type")
    if not isinstance(notice_type, str):
        return
    handler = NOTICE_HANDLERS.get(notice_type)
    if handler is not None:
        await handler(data, bot)


async def receive_onebot_event(
    data: dict[Any, Any],
    bot: ExtBot[None],
    client: httpx.AsyncClient,
) -> None:
    """过滤并转换 OneBot 群消息事件，然后放入内部消息队列。

    这里只负责入口校验和数据建模，不执行 Telegram 网络请求。这样 WebSocket
    接收循环能继续读取 action 响应，不会被慢图片下载或 Telegram API 阻塞。
    """
    if data.get("post_type") == "notice":
        await _receive_onebot_notice(data, bot)
        return

    if data.get("post_type") != "message":
        return

    if data.get("message_type") != "group":
        return

    # notice 子类型是“管理员已禁止匿名聊天”等系统提示，不是用户消息。
    if data.get("sub_type") == "notice":
        return

    user_id = _onebot_int(data.get("user_id"))
    self_id = _onebot_int(data.get("self_id"))
    if user_id is not None and user_id == self_id:
        # 忽略机器人自己发出的消息，防止 Telegram -> OneBot -> Telegram 回环。
        return

    message_id = _onebot_int(data.get("message_id"))
    group_id = _onebot_int(data.get("group_id"))
    segments = _normalize_segments(data.get("message"))
    if (
        message_id is None
        or group_id is None
        or user_id is None
        or self_id is None
        or segments is None
    ):
        qlog.warning("丢弃字段不规范或非数组格式的 OneBot 群消息事件")
        return

    reply_message_id = None
    reply_unavailable = False
    for segment in segments:
        if segment.get("type") != "reply":
            continue
        segment_data = segment.get("data")
        reply_id = segment_data.get("id") if isinstance(segment_data, dict) else None
        if isinstance(reply_id, str):
            try:
                reply_message_id = int(reply_id)
            except ValueError:
                reply_unavailable = True
                emit_runtime_event("capability.succeeded", "onebot.reply.unavailable")
                qlog.warning(
                    "OneBot 回复消息 ID 无法读取: group=%s message=%s",
                    group_id,
                    message_id,
                )
        else:
            reply_message_id = _onebot_int(reply_id)
            if reply_message_id is None:
                reply_unavailable = True
                emit_runtime_event("capability.succeeded", "onebot.reply.unavailable")
                qlog.warning(
                    "OneBot 回复消息 ID 无法读取: group=%s message=%s",
                    group_id,
                    message_id,
                )
        break

    sender_name, sender_name_is_fallback = _sender_name(data, user_id)
    message = OneBotMessage(
        message_id=message_id,
        group_id=group_id,
        user_id=user_id,
        self_id=self_id,
        sender_name=sender_name,
        sender_name_is_fallback=sender_name_is_fallback,
        message=segments,
        reply_message_id=reply_message_id,
        reply_unavailable=reply_unavailable,
    )
    if not begin_onebot_forward(group_id, message_id):
        qlog.warning(
            "忽略重复到达的在途 OneBot 群消息: group=%s message=%s",
            group_id,
            message_id,
        )
        return
    accepted = message_bus.put_nowait(
        onebot_forward_task(message, bot, client, q_gateway)
    )
    if not accepted:
        abandon_onebot_forward(group_id, message_id)
        # action 响应和事件共用 reader；这里不能等待满队列，否则所有 RPC 会超时。
        qlog.error("消息队列已满，丢弃 OneBot 群消息: %s", message_id)
        enqueue_onebot_notice(
            q_gateway,
            q_group_id=group_id,
            text="消息发送到 Telegram 失败：发送队列已满，请稍后重试。",
        )

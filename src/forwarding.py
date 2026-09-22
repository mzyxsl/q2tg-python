from __future__ import annotations

"""两个平台之间的消息转换与发送。"""

import asyncio
import base64
import binascii
import json
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from secrets import token_hex
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote, urlsplit

import filetype
import httpx
from telegram import (
    Bot,
    InputFile,
    InputMediaDocument,
    InputMediaPhoto,
    LinkPreviewOptions,
    ReplyParameters,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError
from telegram.ext import ExtBot
from telegram.helpers import escape_markdown

from src.audio import normalize_onebot_record
from src.config import config
from src.face import (
    normalize_onebot_face_message,
    onebot_super_face_file_id,
    onebot_super_face_id,
    render_onebot_face,
)
from src.log import baselog
from src.mapping_outbox import (
    PendingMessageMapping,
    database_values,
    mapping_outbox,
    newest_mapping,
    newest_pending_mapping,
)
from src.media import (
    TELEGRAM_UPLOAD_LIMIT,
    TELEGRAM_UPLOAD_LIMIT_TEXT,
    MediaFile,
    media_cache,
    media_item_budget,
    media_queue_budget,
)
from src.messages import (
    ONEBOT_USER_NAME,
    FailureAction,
    MediaTooLargeError,
    MessageMappingError,
    OneBotConnectionError,
    OneBotEssenceEvent,
    OneBotGroupBanEvent,
    OneBotGroupMemberEvent,
    OneBotMessage,
    OneBotPokeEvent,
    OneBotResultUnknownError,
    OneBotSendError,
    SendLane,
    SendTarget,
    SendTask,
    TelegramGroupMemberEvent,
    TelegramMessage,
    is_http_url,
    onebot_user_id,
    onebot_user_name,
)
from src.notice import (
    enqueue_bridge_notice,
    enqueue_onebot_notice,
    enqueue_telegram_notice,
)
from src.processing import ProcessingTask
from src.runtime_events import emit_runtime_event
from src.runtime_stats import track_conversion
from src.sql import sql
from src.sticker import static_sticker_to_png, tgs_sticker_to_gif, video_sticker_to_gif
from src.video import normalize_video_for_onebot

if TYPE_CHECKING:
    from src.qbot import QGateway

PHOTO_LIMIT = 10_000_000
ONEBOT_MEDIA_LIMIT = TELEGRAM_UPLOAD_LIMIT
# 云端 Bot API 对单次 multipart 请求体的上限实测为 50 MiB：10 张合计 49.89 MiB
# 通过，50.17 MiB 返回 413 Request Entity Too Large。该上限约束整个请求体，
# 而不是单项，因此媒体组必须按合计字节数拦截。
TELEGRAM_REQUEST_BODY_LIMIT = 50 * 1024 * 1024
# 为 multipart 边界、每项头部、caption 与 media JSON 留出余量；10 项时这部分
# 实际开销不足 10 KB，1 MiB 足够覆盖。
ONEBOT_ALBUM_BYTES_LIMIT = TELEGRAM_REQUEST_BODY_LIMIT - 1024 * 1024
ONEBOT_ALBUM_BYTES_LIMIT_TEXT = f"{ONEBOT_ALBUM_BYTES_LIMIT // (1024 * 1024)} MiB"
DOWNLOAD_CHUNK_SIZE = 64 * 1024
MAX_SEND_ATTEMPTS = 3
UNAVAILABLE_REPLY_TEXT = "[回复了一个无法读取的消息]"
TELEGRAM_VIDEO_LIMIT = 20_000_000
TELEGRAM_CAPTION_LIMIT = 1024
TELEGRAM_TEXT_LIMIT = 4096

_MARKDOWN_IMAGE_TARGET = re.compile(
    r"!\[[^\]\r\n]*\]\\?\((?P<target>[^\r\n]*)"
)
_MARKDOWN_TARGET_URL = re.compile(r"https?://[^\s<>\[\]()]++")
_CQ_MARKDOWN_PREFIX = re.compile(r"\[CQ:markdown[^\r\n]*\)\s*")

# 普通转发和撤回由不同消费者执行。这里记录尚未结束的 OneBot 消息，防止撤回
# 事件任务在普通转发保存映射前查询数据库并错误地当作未命中。
_active_onebot_forwards: set[tuple[int, int]] = set()
_pending_onebot_recalls: set[tuple[int, int]] = set()
_suppressed_onebot_recalls: dict[tuple[int, int], float] = {}
_SUPPRESSED_RECALL_TTL = 120.0


@dataclass(slots=True)
class _TelegramReplacementWaiter:
    """等待同一 Telegram 消息的全部编辑替换任务完成。"""

    event: asyncio.Event
    pending: int = 0


_telegram_replacement_waiters: dict[tuple[int, int], _TelegramReplacementWaiter] = {}


def begin_telegram_replacement(
    tg_chat_id: int,
    tg_message_ids: tuple[int, ...] | list[int],
) -> None:
    """登记编辑消息，避免其后的 Telegram 回复抢先读取旧映射。"""
    for tg_message_id in set(tg_message_ids):
        key = (tg_chat_id, tg_message_id)
        waiter = _telegram_replacement_waiters.get(key)
        if waiter is None:
            waiter = _TelegramReplacementWaiter(event=asyncio.Event())
            _telegram_replacement_waiters[key] = waiter
        waiter.pending += 1


def finish_telegram_replacement(
    tg_chat_id: int,
    tg_message_ids: tuple[int, ...] | list[int],
) -> None:
    """结束编辑替换登记；发送失败时也要唤醒等待者，避免队列永久阻塞。"""
    for tg_message_id in set(tg_message_ids):
        key = (tg_chat_id, tg_message_id)
        waiter = _telegram_replacement_waiters.get(key)
        if waiter is None:
            continue
        waiter.pending -= 1
        if waiter.pending > 0:
            continue
        waiter.event.set()
        _telegram_replacement_waiters.pop(key, None)


async def wait_for_telegram_replacement(tg_chat_id: int, tg_message_id: int) -> None:
    """等待回复目标的编辑替换任务完成后再解析 OneBot 映射。"""
    waiter = _telegram_replacement_waiters.get((tg_chat_id, tg_message_id))
    if waiter is not None:
        await waiter.event.wait()


def suppress_onebot_recall(q_group_id: int, q_message_id: int) -> None:
    """标记桥接自身触发的 QQ 撤回，避免 OneBot 回调删除 Telegram 原消息。"""
    now = asyncio.get_running_loop().time()
    _suppressed_onebot_recalls[(q_group_id, q_message_id)] = now
    expired = [
        key
        for key, created_at in _suppressed_onebot_recalls.items()
        if now - created_at > _SUPPRESSED_RECALL_TTL
    ]
    for key in expired:
        _suppressed_onebot_recalls.pop(key, None)


def consume_suppressed_onebot_recall(q_group_id: int, q_message_id: int) -> bool:
    """消费桥接自身的 QQ 撤回回调；过期标记按普通撤回处理。"""
    created_at = _suppressed_onebot_recalls.pop((q_group_id, q_message_id), None)
    if created_at is None:
        return False
    return asyncio.get_running_loop().time() - created_at <= _SUPPRESSED_RECALL_TTL


def begin_onebot_forward(q_group_id: int, q_message_id: int) -> bool:
    """标记 OneBot 消息已进入普通转发队列；重复在途消息返回 False。"""
    key = (q_group_id, q_message_id)
    if key in _active_onebot_forwards:
        return False
    _active_onebot_forwards.add(key)
    return True


def request_onebot_recall(q_group_id: int, q_message_id: int) -> bool:
    """若消息仍在转发则登记待撤回，并返回 True。"""
    key = (q_group_id, q_message_id)
    if key not in _active_onebot_forwards:
        return False
    _pending_onebot_recalls.add(key)
    return True


def abandon_onebot_forward(q_group_id: int, q_message_id: int) -> None:
    """入口未能入队时清除在途标记。"""
    key = (q_group_id, q_message_id)
    _active_onebot_forwards.discard(key)
    _pending_onebot_recalls.discard(key)


async def onebot_message_text(
    message: list[dict[Any, Any]],
    group_id: int,
    gateway: QGateway | None = None,
    *,
    id_show_enabled: bool = False,
    member_names: dict[int, str | None] | None = None,
    markdown_v2: bool = False,
    self_id: int | None = None,
) -> str:
    """按原顺序拼接 text、face 和可见的 at segment。"""
    if member_names is None:
        member_names = {}
    user_ids: list[int] = []
    for segment in message:
        if segment.get("type") != "at":
            continue
        data = segment.get("data")
        user_id = onebot_user_id(data.get("qq")) if isinstance(data, dict) else None
        if user_id is not None and user_id == self_id:
            continue
        if (
            user_id is not None
            and user_id not in member_names
            and user_id not in user_ids
        ):
            user_ids.append(user_id)
    if user_ids:
        names = await asyncio.gather(
            *(_onebot_member_name(gateway, group_id, user_id) for user_id in user_ids)
        )
        member_names.update(zip(user_ids, names, strict=True))

    parts: list[str] = []
    for segment in message:
        kind = segment.get("type")
        data = segment.get("data")
        if kind == "text":
            text = data.get("text") if isinstance(data, dict) else None
            if isinstance(text, str):
                if "[CQ:markdown" in text or _MARKDOWN_IMAGE_TARGET.search(text):
                    text = _onebot_markdown_text(text)
                parts.append(escape_markdown(text, version=2) if markdown_v2 else text)
            continue
        if kind == "face" and isinstance(data, dict):
            face_id = data.get("id")
            if face_id is not None:
                parts.append(render_onebot_face(face_id))
            continue
        if kind == "markdown":
            content = data.get("content") if isinstance(data, dict) else None
            if isinstance(content, str):
                markdown_text = _onebot_markdown_text(content)
                if markdown_text:
                    parts.append(
                        escape_markdown(markdown_text, version=2)
                        if markdown_v2
                        else markdown_text
                    )
            continue
        if kind != "at" or not isinstance(data, dict):
            continue
        qq = data.get("qq")
        if qq == "all":
            parts.append("@全体成员")
            continue
        user_id = onebot_user_id(qq)
        if user_id is None:
            continue
        if user_id == self_id:
            continue
        name = member_names.get(user_id)
        if name is not None:
            mention = f"{name}[{user_id}]" if id_show_enabled else name
        else:
            mention = str(user_id) if id_show_enabled else ONEBOT_USER_NAME
        mention = f"@{mention}"
        parts.append(escape_markdown(mention, version=2) if markdown_v2 else mention)
    return "".join(parts).strip()


async def _onebot_member_name(
    gateway: QGateway | None,
    group_id: int,
    user_id: int,
    *,
    no_cache: bool = False,
) -> str | None:
    """查询 at 对象的可见名称，失败时返回 None 交给格式层兜底。"""
    if gateway is None:
        return None
    try:
        if no_cache:
            member = await gateway.get_group_member_info(
                group_id,
                user_id,
                no_cache=True,
            )
        else:
            member = await gateway.get_group_member_info(group_id, user_id)
    except Exception:
        baselog.warning(
            "OneBot 群成员信息查询失败: group=%s user=%s",
            group_id,
            user_id,
        )
        return None
    return onebot_user_name(member)


def onebot_message_media(
    message: list[dict[Any, Any]],
) -> tuple[list[tuple[str, str, str]], list[str]]:
    """提取带 HTTP(S) 下载地址的媒体，并返回缺少地址的媒体类型。"""
    # data.url 来自通过 token 认证的可信 OneBot 实现，且常指向 Docker 或局域网
    # 地址，因此这里有意不做 SSRF 私网拦截；部署方必须保护 OneBot 连接凭据。
    media = []
    unavailable: list[str] = []
    for segment in message:
        kind = segment.get("type")
        if kind == "text":
            data = segment.get("data")
            content = data.get("text") if isinstance(data, dict) else None
            if isinstance(content, str):
                for url in _onebot_markdown_image_urls(content):
                    filename = Path(urlsplit(url).path).name or "image"
                    item = ("image", url, filename)
                    if item not in media:
                        media.append(item)
            continue
        if kind == "markdown":
            data = segment.get("data")
            content = data.get("content") if isinstance(data, dict) else None
            if isinstance(content, str):
                for url in _onebot_markdown_image_urls(content):
                    filename = Path(urlsplit(url).path).name or "image"
                    item = ("image", url, filename)
                    if item not in media:
                        media.append(item)
            continue
        if kind not in {"file", "image", "record", "video"}:
            continue
        data = segment.get("data")
        if not isinstance(data, dict):
            unavailable.append(kind)
            continue
        url = data.get("url")
        if not isinstance(url, str) or not is_http_url(url):
            unavailable.append(kind)
            continue
        filename = _onebot_media_filename(data.get("file"), kind)
        media.append((kind, url, filename))
    return media, unavailable


def _onebot_markdown_image_urls(content: str) -> tuple[str, ...]:
    """提取 QQ Markdown 图片地址，并去除嵌套链接语法产生的重复 URL。"""
    content = unquote(content)
    content = _CQ_MARKDOWN_PREFIX.sub("", content, count=1)
    urls: list[str] = []
    for image in _MARKDOWN_IMAGE_TARGET.finditer(content):
        target_urls = _MARKDOWN_TARGET_URL.findall(image.group("target"))
        if not target_urls:
            continue
        # QQ Bot 常生成 ``![alt]([url](url))``；最后一个 URL 是链接目标，
        # 普通 ``![alt](url)`` 也只有这一项。
        url = target_urls[-1].rstrip("\\")
        if is_http_url(url) and url not in urls:
            urls.append(url)
    return tuple(urls)


def _onebot_markdown_text(content: str) -> str:
    """把 Markdown 段中的正文转成不带图片链接的普通文本。"""
    content = unquote(content)
    content = _CQ_MARKDOWN_PREFIX.sub("", content, count=1)
    text = _MARKDOWN_IMAGE_TARGET.sub("", content)
    text = re.sub(r"\[\]\([^\r\n]*\)", "", text)
    text = re.sub(r"\[([^\]\r\n]+)\]\(https?://[^)\r\n]+\)", r"\1", text)
    text = re.sub(r"(?<!\\)(?:\*\*|__|~~|`)", "", text)
    return text.replace(r"\(", "(").replace(r"\)", ")").strip()


def onebot_group_announcement(
    message: list[dict[Any, Any]],
) -> tuple[str, str | None] | None:
    """从 Tencent 群公告 JSON 消息段提取正文和首张图片地址。"""
    for segment in message:
        if segment.get("type") != "json":
            continue
        data = segment.get("data")
        payload = data.get("data") if isinstance(data, dict) else None
        if not isinstance(payload, str):
            continue
        try:
            card = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(card, dict) or card.get("app") != "com.tencent.mannounce":
            continue
        meta = card.get("meta")
        announcement = meta.get("mannounce") if isinstance(meta, dict) else None
        if not isinstance(announcement, dict):
            continue
        text = announcement.get("text")
        if not isinstance(text, str):
            continue
        if announcement.get("encode") == 1:
            try:
                text = base64.b64decode(text, validate=True).decode("utf-8")
            except (binascii.Error, UnicodeDecodeError):
                continue
        image_url = None
        pictures = announcement.get("pic")
        if isinstance(pictures, list) and pictures:
            picture = pictures[0]
            image_id = picture.get("url") if isinstance(picture, dict) else None
            if isinstance(image_id, str) and (image_id := image_id.strip()):
                image_url = (
                    "https://gdynamic.qpic.cn/gdynamic/"
                    f"{quote(image_id, safe='')}/0"
                )
        return text, image_url
    return None


def _onebot_media_filename(file: Any, kind: str) -> str:
    """把入站 data.file 作为文件名，并移除不适合上传文件名的字符。"""
    if isinstance(file, str):
        filename = file.strip().replace("\x00", "").replace("\r", "").replace("\n", "")
        # file 在入站消息中是文件名，不是本地路径；分隔符只做安全替换。
        filename = filename.replace("/", "_")
        if filename and filename not in {".", ".."}:
            return filename
    if kind == "video":
        return "video.mp4"
    if kind == "record":
        return "voice.silk"
    return "image" if kind == "image" else "file"


async def download_media(
    client: httpx.AsyncClient,
    url: str,
    *,
    filename: str,
    kind: str,
) -> MediaFile:
    """把 OneBot 媒体流式下载到 spool，失败时关闭文件。"""
    fallback_type = {
        "image": "image/jpeg",
        "record": "audio/silk",
        "video": "video/mp4",
    }.get(kind, "application/octet-stream")
    # spool 需要 Content-Length 才能决定内存分档，所以文件要等到读出响应头才创建。
    # 但文件名额必须在建立连接之前取得：ItemBudget.acquire() 会等待，若放到流内部，
    # 名额耗尽时就会挂着一条已打开的 OneBot 连接干等。
    await media_item_budget.acquire()
    media: MediaFile | None = None
    try:
        try:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                response_type = response.headers.get("content-type", "").partition(";")[
                    0
                ]
                content_length = response.headers.get("content-length")
                declared_size = (
                    int(content_length) if content_length is not None else None
                )
                if declared_size is not None and declared_size > ONEBOT_MEDIA_LIMIT:
                    raise MediaTooLargeError(
                        f"OneBot 媒体超过 {TELEGRAM_UPLOAD_LIMIT_TEXT}，无法转发"
                    )
                # 名额已在上面取得，这里用同步的 create_reserved 消耗它。
                media = MediaFile.create_reserved(
                    filename=filename,
                    media_type=response_type or fallback_type,
                    expected_size=declared_size,
                )
                async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_SIZE):
                    if media.size + len(chunk) > ONEBOT_MEDIA_LIMIT:
                        raise MediaTooLargeError(
                            f"OneBot 媒体超过 {TELEGRAM_UPLOAD_LIMIT_TEXT}，无法转发"
                        )
                    media.write(chunk)
                media.rewind()
                baselog.info(
                    "OneBot 媒体下载完成：kind=%s filename=%s 实际 %d 字节"
                    "（声明 %s，类型 %s）",
                    kind,
                    media.filename,
                    media.size,
                    declared_size if declared_size is not None else "未提供",
                    media.media_type,
                )
                return media
        except httpx.HTTPError:
            raise RuntimeError("OneBot 媒体下载失败") from None
    except BaseException:
        if media is not None:
            # MediaFile 已接管名额，关闭时归还。
            media.close()
        else:
            media_item_budget.release()
        raise


async def download_image(
    client: httpx.AsyncClient,
    url: str,
    *,
    filename: str,
) -> MediaFile:
    """保留明确的图片下载入口供调用方和测试使用。"""
    return await download_media(client, url, filename=filename, kind="image")


async def forward_onebot_to_telegram(
    msg: OneBotMessage,
    bot: ExtBot[None],
    client: httpx.AsyncClient,
    gateway: QGateway | None = None,
) -> None:
    """将 OneBot 文本和图片按顺序发送到绑定的 Telegram 群。"""
    if msg.tg_forward_complete:
        await _save_onebot_message_mapping(msg)
        return
    existing_mapping = await _get_tg_message(msg.group_id, msg.message_id)
    if existing_mapping is not None:
        msg.tg_chat_id = existing_mapping.tg_chat_id
        msg.tg_message_ids.extend(existing_mapping.tg_message_ids)
        baselog.warning(
            "忽略已有 Telegram 映射的重复 OneBot 消息: group=%s message=%s",
            msg.group_id,
            msg.message_id,
        )
        return
    group_id = await sql.get_tg_group(msg.group_id)
    if group_id is None:
        baselog.warning("OneBot 群未配置转发目标: %s", msg.group_id)
        return
    if not await sql.get_tg_forward_enabled(group_id):
        return
    msg.tg_chat_id = group_id

    normalized_message = normalize_onebot_face_message(msg.message)
    super_face_id = onebot_super_face_id(normalized_message)

    sender_name = msg.sender_name
    id_show_enabled = bool(await sql.get_id_show_enabled(group_id))
    has_faces = any(segment.get("type") == "face" for segment in normalized_message)
    has_forwards = any(segment.get("type") == "forward" for segment in normalized_message)
    announcement = onebot_group_announcement(normalized_message)
    markdown_v2 = has_faces or has_forwards
    parse_mode = ParseMode.MARKDOWN_V2 if markdown_v2 else None
    text = await onebot_message_text(
        normalized_message,
        msg.group_id,
        gateway,
        id_show_enabled=id_show_enabled,
        member_names=msg.mention_names,
        markdown_v2=markdown_v2,
        self_id=msg.self_id,
    )
    forward_links: list[str] = []
    for segment in normalized_message:
        if segment.get("type") != "forward":
            continue
        segment_data = segment.get("data")
        forward_id = segment_data.get("id") if isinstance(segment_data, dict) else None
        if not isinstance(forward_id, str) or not forward_id:
            continue
        page = msg.telegraph_pages.get(forward_id)
        if page is None:
            from src.onebot_forward import create_forward_page
            from src.telegraph_client import telegraph_client

            if gateway is None:
                raise RuntimeError("OneBot 合并转发缺少可用的 gateway")
            page = await create_forward_page(
                forward_id,
                gateway,
                telegraph_client,
            )
            msg.telegraph_pages[forward_id] = page
        link_title = escape_markdown(page.title, version=2)
        forward_links.append(f"[{link_title}]({page.url})")
    if forward_links:
        forward_text = "\n".join(forward_links)
        text = f"{text}\n{forward_text}" if text else forward_text
    if msg.sender_name_is_fallback and not id_show_enabled:
        sender_name = ONEBOT_USER_NAME
    elif not msg.sender_name_is_fallback and id_show_enabled:
        sender_name = f"{sender_name}[{msg.user_id}]"
    if markdown_v2 and super_face_id is None:
        sender_name = escape_markdown(sender_name, version=2)
    media, unavailable = onebot_message_media(normalized_message)
    if not text and not media and not unavailable and announcement is None:
        baselog.warning("OneBot 消息没有可转发的内容: %s", msg.message_id)
        return

    caption = f"{sender_name}:\n{text}" if text else f"{sender_name}:"
    if unavailable:
        baselog.warning(
            "OneBot 媒体缺少可用下载地址，使用提示文本: group=%s message=%s types=%s",
            msg.group_id,
            msg.message_id,
            ",".join(dict.fromkeys(unavailable)),
        )
        labels = {"image": "图片", "video": "视频", "record": "语音", "file": "文件"}
        notices = [
            f"[{labels[kind]}无法转发：缺少可用的 HTTP(S) 下载地址]"
            for kind in dict.fromkeys(unavailable)
        ]
        notice_text = "\n".join(notices)
        caption += "\n" + (
            escape_markdown(notice_text, version=2) if markdown_v2 else notice_text
        )
    reply_parameters = None
    if msg.reply_message_id is not None:
        reply_mapping = await _get_tg_message(msg.group_id, msg.reply_message_id)
        if reply_mapping is not None and reply_mapping.tg_message_ids:
            msg.reply_unavailable = False
            emit_runtime_event("capability.succeeded", "onebot.reply.mapped")
            reply_parameters = ReplyParameters(
                message_id=reply_mapping.tg_message_ids[0],
            )
        else:
            emit_runtime_event("capability.succeeded", "onebot.reply.unavailable")
            msg.reply_unavailable = True
            baselog.warning(
                "OneBot 回复映射不存在，使用提示文本: group=%s message=%s reply=%s",
                msg.group_id,
                msg.message_id,
                msg.reply_message_id,
            )
    if msg.reply_unavailable:
        fallback = escape_markdown(UNAVAILABLE_REPLY_TEXT, version=2) if markdown_v2 else UNAVAILABLE_REPLY_TEXT
        caption = f"{caption}\n{fallback}"

    if announcement is not None:
        emit_runtime_event("capability.succeeded", "onebot.announcement")
        await _forward_announcement(
            msg,
            bot,
            client,
            group_id,
            announcement,
            caption if msg.reply_unavailable else f"{sender_name}:",
            reply_parameters,
        )
    elif super_face_id is not None and not has_forwards:
        await _forward_super_face(
            msg, bot, group_id, super_face_id, sender_name, caption, reply_parameters, parse_mode
        )
    elif not media:
        await _send_text_chunks(
            msg,
            bot,
            group_id,
            caption,
            reply_parameters,
            parse_mode=parse_mode,
        )
    elif (
        msg.next_media_index == 0
        and 2 <= len(media) <= 10
        and all(kind == "image" for kind, _, _ in media)
    ):
        await _forward_media_album(
            msg, bot, client, group_id, media, caption, reply_parameters, parse_mode
        )
    else:
        await _forward_media_sequential(
            msg, bot, client, group_id, media, caption, reply_parameters, parse_mode
        )

    msg.tg_forward_complete = True
    await _save_onebot_message_mapping(msg)


async def _save_onebot_message_mapping(msg: OneBotMessage) -> None:
    if msg.tg_chat_id is None:
        raise RuntimeError("Telegram 转发完成但缺少目标群 ID")
    await _save_or_queue_message_mapping(
        PendingMessageMapping(
            q_group_id=msg.group_id,
            q_message_ids=(msg.message_id,),
            tg_chat_id=msg.tg_chat_id,
            tg_message_ids=tuple(msg.tg_message_ids),
            q_user_id=msg.user_id,
        ),
        error_message="Telegram 消息发送成功，但消息映射保存失败",
    )


async def _forward_announcement(
    msg: OneBotMessage,
    bot: ExtBot[None],
    client: httpx.AsyncClient,
    group_id: int,
    announcement: tuple[str, str | None],
    caption: str,
    reply_parameters: ReplyParameters | None,
) -> None:
    """把 OneBot 群公告正文作为文档发送，并在其后附加首张公告图片。"""
    body, image_url = announcement
    if msg.announcement_filename is None:
        msg.announcement_filename = f"群公告 - {token_hex(8)}.md"
    if not msg.tg_message_ids:
        # 公告文件可能被下载和长期保存，正文作者永久隐藏数字 ID；Telegram
        # caption 仍沿用上方按当前群 id_show 设置生成的 sender_name。
        author = ONEBOT_USER_NAME if msg.sender_name_is_fallback else msg.sender_name
        content = f"{author}:\n\n# 群公告\n\n{body}\n".encode()
        sent = await bot.send_document(
            chat_id=group_id,
            document=InputFile(content, filename=msg.announcement_filename),
            caption=caption,
            reply_parameters=reply_parameters,
        )
        msg.tg_message_ids.append(sent.message_id)
    if image_url is not None and len(msg.tg_message_ids) == 1:
        image = await download_image(
            client,
            image_url,
            filename="群公告图片.jpg",
        )
        try:
            upload = InputFile(
                image.file,
                filename=image.filename,
                read_file_handle=False,
            )
            image_reply = ReplyParameters(
                message_id=msg.tg_message_ids[0],
            )
            if image.size <= PHOTO_LIMIT:
                sent = await bot.send_photo(
                    chat_id=group_id,
                    photo=upload,
                    reply_parameters=image_reply,
                )
            else:
                sent = await bot.send_document(
                    chat_id=group_id,
                    document=upload,
                    reply_parameters=image_reply,
                )
            msg.tg_message_ids.append(sent.message_id)
        finally:
            image.close()


async def _forward_super_face(
    msg: OneBotMessage,
    bot: ExtBot[None],
    group_id: int,
    super_face_id: str,
    sender_name: str,
    caption: str,
    reply_parameters: ReplyParameters | None,
    parse_mode: ParseMode | None,
) -> None:
    """发送超级表情对应的 Telegram Sticker；缺失映射时回退为文本。"""
    sticker_file_id = await onebot_super_face_file_id(bot, super_face_id)
    if sticker_file_id is not None:
        emit_runtime_event("capability.succeeded", "onebot.face.super")
        if not msg.tg_message_ids:
            sent_header = await bot.send_message(
                chat_id=group_id,
                text=caption if msg.reply_unavailable else f"{sender_name}:",
                reply_parameters=reply_parameters,
            )
            msg.tg_message_ids.append(sent_header.message_id)
        sent_sticker = await bot.send_sticker(
            chat_id=group_id,
            sticker=sticker_file_id,
        )
        msg.tg_message_ids.append(sent_sticker.message_id)
    else:
        baselog.warning(
            "OneBot 超级表情缺少 Telegram Sticker 映射，使用文本: group=%s message=%s",
            msg.group_id,
            msg.message_id,
        )
        await _send_text_chunks(
            msg,
            bot,
            group_id,
            caption,
            reply_parameters,
            parse_mode=parse_mode,
        )


async def _forward_media_album(
    msg: OneBotMessage,
    bot: ExtBot[None],
    client: httpx.AsyncClient,
    group_id: int,
    media: list[tuple[str, str, str]],
    caption: str,
    reply_parameters: ReplyParameters | None,
    parse_mode: ParseMode | None,
) -> None:
    """把 2~10 张图片作为 Telegram 媒体组发送；含 GIF 时逐条发送。"""
    media_caption, media_reply = await _prepare_media_caption(
        msg,
        bot,
        group_id,
        caption,
        reply_parameters,
        parse_mode=parse_mode,
    )
    contents: list[MediaFile] = []
    try:
        for kind, url, filename in media:
            contents.append(
                await download_media(
                    client,
                    url,
                    filename=filename,
                    kind=kind,
                )
            )
            # 逐张累计判断，超限时立即停止，不再继续下载后续图片。
            if sum(content.size for content in contents) > ONEBOT_ALBUM_BYTES_LIMIT:
                raise MediaTooLargeError(
                    f"OneBot 图片组超过 {ONEBOT_ALBUM_BYTES_LIMIT_TEXT}，无法转发"
                )
        as_animations = any(_is_gif(content) for content in contents)
        as_photos = all(content.size <= PHOTO_LIMIT for content in contents)
        album: list[InputMediaPhoto | InputMediaDocument] = []
        if as_animations:
            await _send_downloaded_media_individually(
                msg,
                bot,
                group_id,
                media,
                contents,
                media_caption,
                media_reply,
                parse_mode,
            )
        elif as_photos:
            emit_runtime_event("capability.succeeded", "onebot.image-album.photo")
            album = [
                InputMediaPhoto(
                    media=InputFile(
                        content.file,
                        filename=filename,
                        attach=True,
                        read_file_handle=False,
                    ),
                    caption=media_caption if index == 0 else None,
                    parse_mode=parse_mode,
                    # Telegram 要求媒体组内所有项的 show_caption_above_media 一致；
                    # caption 只放首项，但该标志必须全组统一，否则整组被拒。
                    show_caption_above_media=True,
                )
                for index, (content, (_, _, filename)) in enumerate(
                    zip(contents, media, strict=True)
                )
            ]
        else:
            emit_runtime_event("capability.succeeded", "onebot.image-album.document")
            album = [
                InputMediaDocument(
                    media=InputFile(
                        content.file,
                        filename=filename,
                        attach=True,
                        read_file_handle=False,
                    ),
                    caption=media_caption if index == len(contents) - 1 else None,
                    parse_mode=parse_mode,
                )
                for index, (content, (_, _, filename)) in enumerate(
                    zip(contents, media, strict=True)
                )
            ]
        if not as_animations:
            sent_messages = await bot.send_media_group(
                chat_id=group_id,
                media=album,
                reply_parameters=media_reply,
            )
            msg.tg_message_ids.extend(sent.message_id for sent in sent_messages)
            msg.next_media_index = len(media)
    finally:
        for content in contents:
            content.close()


async def _forward_media_sequential(
    msg: OneBotMessage,
    bot: ExtBot[None],
    client: httpx.AsyncClient,
    group_id: int,
    media: list[tuple[str, str, str]],
    caption: str,
    reply_parameters: ReplyParameters | None,
    parse_mode: ParseMode | None,
) -> None:
    """逐条下载并发送媒体，支持断点续发与语音规范化。"""
    media_caption, media_reply = await _prepare_media_caption(
        msg,
        bot,
        group_id,
        caption,
        reply_parameters,
        parse_mode=parse_mode,
    )
    for index in range(msg.next_media_index, len(media)):
        kind, url, filename = media[index]
        content = await download_media(
            client,
            url,
            filename=filename,
            kind=kind,
        )
        try:
            if kind == "record":
                await track_conversion("voice", normalize_onebot_record(content))
            await _send_single_media(
                msg,
                bot,
                group_id,
                content,
                kind,
                filename,
                media_caption,
                media_reply,
                parse_mode,
            )
            msg.next_media_index = index + 1
        finally:
            content.close()


async def recall_onebot_message_from_telegram(
    q_group_id: int,
    q_message_id: int,
    bot: ExtBot[None],
    *,
    tg_chat_id: int | None = None,
    tg_message_ids: tuple[int, ...] = (),
) -> None:
    """根据 OneBot 消息映射删除 Telegram 侧的全部副本。"""
    if tg_chat_id is None or not tg_message_ids:
        mapping = await _get_tg_message(q_group_id, q_message_id)
        if mapping is None:
            baselog.warning(
                "OneBot 撤回事件没有可用的 Telegram 消息映射: group=%s message=%s",
                q_group_id,
                q_message_id,
            )
            return
        tg_chat_id = mapping.tg_chat_id
        tg_message_ids = mapping.tg_message_ids
    if not await sql.get_tg_forward_enabled(tg_chat_id):
        return
    for index in range(0, len(tg_message_ids), 100):
        await bot.delete_messages(
            chat_id=tg_chat_id,
            message_ids=tg_message_ids[index : index + 100],
        )


async def forward_onebot_essence_to_telegram(
    event: OneBotEssenceEvent,
    bot: ExtBot[None],
) -> None:
    """把 OneBot 精华状态应用到映射中的全部 Telegram 消息。"""
    mapping = await _get_tg_message(event.group_id, event.message_id)
    if mapping is None:
        baselog.warning(
            "OneBot 精华事件没有可用的 Telegram 消息映射: group=%s message=%s",
            event.group_id,
            event.message_id,
        )
        return
    if not await sql.get_tg_forward_enabled(mapping.tg_chat_id):
        return
    for message_id in mapping.tg_message_ids:
        if event.added:
            await bot.pin_chat_message(
                chat_id=mapping.tg_chat_id,
                message_id=message_id,
                disable_notification=True,
            )
        else:
            await bot.unpin_chat_message(
                chat_id=mapping.tg_chat_id,
                message_id=message_id,
            )


def onebot_essence_task(
    event: OneBotEssenceEvent,
    bot: ExtBot[None],
) -> SendTask:
    """创建发送到 Telegram 事件队列的精华状态任务。"""
    action = "add" if event.added else "delete"
    return SendTask(
        target=SendTarget.TELEGRAM,
        lane=SendLane.EVENT,
        send=partial(forward_onebot_essence_to_telegram, event, bot),
        failure_action=_telegram_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"onebot-essence-{action}:{event.group_id}:{event.message_id}",
    )


def format_duration(seconds: int) -> str:
    """把非负秒数缩放为天、小时、分钟和秒的中文组合。"""
    units = ((86_400, "天"), (3_600, "小时"), (60, "分钟"), (1, "秒"))
    parts: list[str] = []
    remainder = seconds
    for unit_seconds, label in units:
        value, remainder = divmod(remainder, unit_seconds)
        if value:
            parts.append(f"{value} {label}")
    return " ".join(parts) or "0 秒"


def _event_member_text(name: str | None, user_id: int, *, show_id: bool) -> str:
    if name is None:
        baselog.warning(
            "OneBot 群成员名称不可用，使用通用名称: user=%s show_id=%s",
            user_id,
            show_id,
        )
        return str(user_id) if show_id else ONEBOT_USER_NAME
    return f"{name}[{user_id}]" if show_id else name


async def forward_onebot_group_ban_to_telegram(
    event: OneBotGroupBanEvent,
    bot: ExtBot[None],
    gateway: QGateway,
) -> None:
    """把 OneBot 群禁言事件格式化为 Telegram 文本消息。"""
    tg_chat_id = await sql.get_tg_group(event.group_id)
    if tg_chat_id is None:
        baselog.warning("OneBot 群事件未配置转发目标: %s", event.group_id)
        return
    if not await sql.get_tg_forward_enabled(tg_chat_id):
        return
    show_id = bool(await sql.get_id_show_enabled(tg_chat_id))
    user_name, operator_name = await asyncio.gather(
        _onebot_member_name(gateway, event.group_id, event.user_id),
        _onebot_member_name(gateway, event.group_id, event.operator_id),
    )
    user = _event_member_text(user_name, event.user_id, show_id=show_id)
    operator = _event_member_text(
        operator_name,
        event.operator_id,
        show_id=show_id,
    )
    if event.lifted:
        text = f"{user} 被管理员 {operator} 解除禁言"
    else:
        text = f"{user} 被管理员 {operator} 禁言 {format_duration(event.duration)}"
    await bot.send_message(
        chat_id=tg_chat_id,
        text=text,
        disable_notification=True,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


def onebot_group_ban_task(
    event: OneBotGroupBanEvent,
    bot: ExtBot[None],
    gateway: QGateway,
) -> SendTask:
    """创建发送到 Telegram 事件队列的群禁言任务。"""
    return SendTask(
        target=SendTarget.TELEGRAM,
        lane=SendLane.EVENT,
        send=partial(forward_onebot_group_ban_to_telegram, event, bot, gateway),
        failure_action=_telegram_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"onebot-group-ban:{event.group_id}:{event.user_id}",
    )


async def forward_onebot_group_member_to_telegram(
    event: OneBotGroupMemberEvent,
    bot: ExtBot[None],
    gateway: QGateway,
) -> None:
    """把 OneBot 群成员加入或退出事件格式化为 Telegram 文本消息。"""
    tg_chat_id = await sql.get_tg_group(event.group_id)
    if tg_chat_id is None:
        baselog.warning("OneBot 群事件未配置转发目标: %s", event.group_id)
        return
    if not await sql.get_tg_forward_enabled(tg_chat_id):
        return
    show_id = bool(await sql.get_id_show_enabled(tg_chat_id))
    name = await _onebot_member_name(
        gateway,
        event.group_id,
        event.user_id,
        no_cache=event.joined,
    )
    user = _event_member_text(name, event.user_id, show_id=show_id)
    action = "加入群聊" if event.joined else "退出群聊"
    await bot.send_message(
        chat_id=tg_chat_id,
        text=f"{user} {action}",
        disable_notification=True,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


def onebot_group_member_task(
    event: OneBotGroupMemberEvent,
    bot: ExtBot[None],
    gateway: QGateway,
) -> SendTask:
    """创建发送到 Telegram 事件队列的群成员变动任务。"""
    action = "increase" if event.joined else "decrease"
    return SendTask(
        target=SendTarget.TELEGRAM,
        lane=SendLane.EVENT,
        send=partial(forward_onebot_group_member_to_telegram, event, bot, gateway),
        failure_action=_telegram_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"onebot-group-{action}:{event.group_id}:{event.user_id}",
    )


async def forward_onebot_poke_to_telegram(
    event: OneBotPokeEvent,
    bot: ExtBot[None],
    gateway: QGateway,
) -> None:
    """把 OneBot 群戳一戳事件格式化为 Telegram 文本消息。"""
    tg_chat_id = await sql.get_tg_group(event.group_id)
    if tg_chat_id is None:
        baselog.warning("OneBot 群事件未配置转发目标: %s", event.group_id)
        return
    if not await sql.get_tg_forward_enabled(tg_chat_id):
        return
    show_id = bool(await sql.get_id_show_enabled(tg_chat_id))
    if event.user_id == event.target_id:
        user_name = await _onebot_member_name(gateway, event.group_id, event.user_id)
        target_name = None
    else:
        user_name, target_name = await asyncio.gather(
            _onebot_member_name(gateway, event.group_id, event.user_id),
            _onebot_member_name(gateway, event.group_id, event.target_id),
        )
    user = _event_member_text(user_name, event.user_id, show_id=show_id)
    target = (
        "自己"
        if event.user_id == event.target_id
        else _event_member_text(target_name, event.target_id, show_id=show_id)
    )
    await bot.send_message(
        chat_id=tg_chat_id,
        text=f"{user} {event.action} {target} {event.suffix}",
        disable_notification=True,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


def onebot_poke_task(
    event: OneBotPokeEvent,
    bot: ExtBot[None],
    gateway: QGateway,
) -> SendTask:
    """创建发送到 Telegram 事件队列的戳一戳任务。"""
    return SendTask(
        target=SendTarget.TELEGRAM,
        lane=SendLane.EVENT,
        send=partial(forward_onebot_poke_to_telegram, event, bot, gateway),
        failure_action=_telegram_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"onebot-poke:{event.group_id}:{event.user_id}:{event.target_id}",
    )


def onebot_recall_task(
    q_group_id: int,
    q_message_id: int,
    bot: ExtBot[None],
    *,
    tg_chat_id: int | None = None,
    tg_message_ids: tuple[int, ...] = (),
) -> SendTask:
    """创建与普通 OneBot 消息同队列、同重试策略的 Telegram 撤回任务。"""
    return SendTask(
        target=SendTarget.TELEGRAM,
        lane=SendLane.EVENT,
        send=partial(
            recall_onebot_message_from_telegram,
            q_group_id,
            q_message_id,
            bot,
            tg_chat_id=tg_chat_id,
            tg_message_ids=tg_message_ids,
        ),
        failure_action=_telegram_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"onebot-recall:{q_group_id}:{q_message_id}",
    )


async def finalize_onebot_forward(msg: OneBotMessage, bot: ExtBot[None]) -> None:
    """结束在途状态，并把等待中的撤回交给事件队列。"""
    from src.bus import message_bus

    key = (msg.group_id, msg.message_id)
    if key not in _pending_onebot_recalls:
        _active_onebot_forwards.discard(key)
        return
    if msg.tg_chat_id is None or not msg.tg_message_ids:
        _active_onebot_forwards.discard(key)
        _pending_onebot_recalls.discard(key)
        return
    await message_bus.put(
        onebot_recall_task(
            msg.group_id,
            msg.message_id,
            bot,
            tg_chat_id=msg.tg_chat_id,
            tg_message_ids=tuple(msg.tg_message_ids),
        )
    )
    _active_onebot_forwards.discard(key)
    _pending_onebot_recalls.discard(key)


async def _send_downloaded_media_individually(
    msg: OneBotMessage,
    bot: ExtBot[None],
    group_id: int,
    media: list[tuple[str, str, str]],
    contents: list[MediaFile],
    caption: str | None,
    reply_parameters: ReplyParameters | None,
    parse_mode: ParseMode | None,
) -> None:
    """按原顺序发送不能组成 Telegram 媒体组的已下载媒体。"""
    for index, (content, (kind, _, filename)) in enumerate(
        zip(contents, media, strict=True)
    ):
        await _send_single_media(
            msg,
            bot,
            group_id,
            content,
            kind,
            filename,
            caption,
            reply_parameters,
            parse_mode,
        )
        msg.next_media_index = index + 1


async def _send_single_media(
    msg: OneBotMessage,
    bot: ExtBot[None],
    group_id: int,
    content: MediaFile,
    kind: str,
    filename: str,
    caption: str | None,
    reply_parameters: ReplyParameters | None,
    parse_mode: ParseMode | None,
) -> None:
    """发送单个已下载媒体，并按媒体类型选择合适的 Telegram 发送方法。

    caption 与 reply 只在本条 OneBot 消息尚未发出任何 Telegram 消息时附加，
    使后续媒体不会重复附带作者与引用信息。
    """
    is_gif = kind == "image" and _is_gif(content)
    if kind == "record":
        upload_filename = content.filename
    elif is_gif:
        upload_filename = f"{Path(filename).stem or 'animation'}.gif"
    else:
        upload_filename = filename
    upload = InputFile(content.file, filename=upload_filename, read_file_handle=False)
    item_caption = caption if not msg.tg_message_ids else None
    item_reply = reply_parameters if not msg.tg_message_ids else None
    if kind == "record":
        sent = await bot.send_voice(
            chat_id=group_id,
            voice=upload,
            caption=item_caption,
            parse_mode=parse_mode,
            reply_parameters=item_reply,
        )
    elif is_gif:
        emit_runtime_event("capability.succeeded", "onebot.image.animation")
        sent = await bot.send_animation(
            chat_id=group_id,
            animation=upload,
            caption=item_caption,
            parse_mode=parse_mode,
            show_caption_above_media=True,
            reply_parameters=item_reply,
        )
    elif kind == "image" and content.size <= PHOTO_LIMIT:
        emit_runtime_event("capability.succeeded", "onebot.image.photo")
        sent = await bot.send_photo(
            chat_id=group_id,
            photo=upload,
            caption=item_caption,
            parse_mode=parse_mode,
            show_caption_above_media=True,
            reply_parameters=item_reply,
        )
    elif kind == "video" and _is_mp4(content):
        emit_runtime_event("capability.succeeded", "onebot.media.video")
        sent = await bot.send_video(
            chat_id=group_id,
            video=upload,
            caption=item_caption,
            parse_mode=parse_mode,
            show_caption_above_media=True,
            supports_streaming=True,
            reply_parameters=item_reply,
        )
    else:
        if kind == "image":
            emit_runtime_event("capability.succeeded", "onebot.image.document")
        elif kind == "file":
            emit_runtime_event("capability.succeeded", "onebot.media.file")
        sent = await bot.send_document(
            chat_id=group_id,
            document=upload,
            caption=item_caption,
            parse_mode=parse_mode,
            reply_parameters=item_reply,
        )
    msg.tg_message_ids.append(sent.message_id)


async def _prepare_media_caption(
    msg: OneBotMessage,
    bot: ExtBot[None],
    group_id: int,
    caption: str,
    reply_parameters: ReplyParameters | None,
    *,
    parse_mode: ParseMode | None,
) -> tuple[str | None, ReplyParameters | None]:
    if _utf16_length(caption) <= TELEGRAM_CAPTION_LIMIT:
        return caption, reply_parameters
    await _send_text_chunks(
        msg,
        bot,
        group_id,
        caption,
        reply_parameters,
        parse_mode=parse_mode,
    )
    return None, None


async def _send_text_chunks(
    msg: OneBotMessage,
    bot: ExtBot[None],
    group_id: int,
    text: str,
    reply_parameters: ReplyParameters | None,
    *,
    parse_mode: ParseMode | None = None,
) -> None:
    chunks = _split_telegram_text(text)
    for index in range(msg.next_text_chunk_index, len(chunks)):
        send_options: dict[str, Any] = {}
        if parse_mode is not None:
            send_options["parse_mode"] = parse_mode
        sent = await bot.send_message(
            chat_id=group_id,
            text=chunks[index],
            reply_parameters=reply_parameters if index == 0 else None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            **send_options,
        )
        msg.tg_message_ids.append(sent.message_id)
        msg.next_text_chunk_index = index + 1


def _split_telegram_text(text: str) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    units = 0
    for character in text:
        character_units = _utf16_length(character)
        if current and units + character_units > TELEGRAM_TEXT_LIMIT:
            chunks.append("".join(current))
            current = []
            units = 0
        current.append(character)
        units += character_units
    if current:
        chunks.append("".join(current))
    return chunks


def _utf16_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


async def forward_telegram_to_onebot(msg: TelegramMessage, gateway: QGateway) -> None:
    """将 Telegram 文本或图片通过 OneBot action 发送到绑定的 OneBot 群。"""
    if msg.q_forward_complete:
        await _save_telegram_message_mapping(msg)
        return
    group_id = await sql.get_q_group(msg.group_id)
    if group_id is None:
        baselog.warning("Telegram 群未配置转发目标: %s", msg.group_id)
        return
    msg.q_group_id = group_id
    if not await sql.get_tg_forward_enabled(msg.group_id):
        return
    if msg.bot_forward_required and not await sql.get_bot_forward_enabled(msg.group_id):
        return
    if not msg.text and not msg.media and msg.at_user_id is None:
        baselog.warning("Telegram 消息没有可转发的内容: %s", msg.message_ids)
        return

    if msg.replace_existing and not msg.replacement_done:
        await _recall_replaced_onebot_messages(msg, gateway)

    reply_q_message_id = None
    if msg.reply_message_id is not None:
        await wait_for_telegram_replacement(msg.group_id, msg.reply_message_id)
        reply_mapping = await _get_q_message(msg.group_id, msg.reply_message_id)
        if reply_mapping is not None and reply_mapping.q_message_ids:
            emit_runtime_event("capability.succeeded", "telegram.reply.mapped")
            reply_q_message_id = reply_mapping.q_message_ids[-1]
        else:
            emit_runtime_event("capability.succeeded", "telegram.reply.unavailable")
            baselog.warning(
                "Telegram 回复映射不存在，使用提示文本: chat=%s messages=%s reply=%s",
                msg.group_id,
                msg.message_ids,
                msg.reply_message_id,
            )

    batches: list[list[dict[str, Any]]]
    text = f"{msg.sender_name}:"
    if msg.forwarded_from is not None:
        text += f"\n转发自: {msg.forwarded_from}"
    if msg.at_user_id is not None:
        text += "\n"
    elif msg.text:
        text += f"\n{msg.text}"
    if msg.reply_message_id is not None and reply_q_message_id is None:
        text += f"\n{UNAVAILABLE_REPLY_TEXT}"
    if msg.media:
        if msg.media_ids is None:
            msg.media_ids = media_cache.set_media_batch(
                tuple(attachment.content for attachment in msg.media),
                pinned=True,
            )
            msg.media_cache_pinned = True
        text_segment: dict[str, str | dict[str, str]] = {
            "type": "text",
            "data": {"text": text},
        }
        media_segments: list[dict[str, Any]] = []
        for attachment, media_id in zip(msg.media, msg.media_ids, strict=True):
            media_url = f"{config.onebot_media_url}/media/{media_id}"
            data = {"file": media_url}
            if attachment.kind == "file":
                data["name"] = attachment.content.filename
            media_segments.append({"type": attachment.kind, "data": data})
        if any(attachment.kind in {"record", "video"} for attachment in msg.media):
            # SnowLuma 的 video 和 record 都会丢失同 action 的文本
            batches = [[text_segment], media_segments]
        else:
            batches = [[text_segment, *media_segments]]
    else:
        segments: list[dict[str, Any]] = [
            {"type": "text", "data": {"text": text}},
        ]
        if msg.at_user_id is not None:
            segments.append(
                {"type": "at", "data": {"qq": str(msg.at_user_id)}}
            )
        batches = [segments]

    if reply_q_message_id is not None:
        batches[0].insert(
            0,
            {"type": "reply", "data": {"id": str(reply_q_message_id)}},
        )

    for index in range(msg.next_onebot_batch, len(batches)):
        try:
            message_id = await gateway.send_group_message(
                group_id=group_id,
                message=batches[index],
            )
        except OneBotConnectionError:
            raise
        except OneBotResultUnknownError:
            raise
        except Exception as error:
            raise OneBotSendError from error
        msg.q_message_ids.append(message_id)
        msg.next_onebot_batch = index + 1

    msg.q_forward_complete = True
    await _save_telegram_message_mapping(msg)


async def _recall_replaced_onebot_messages(
    msg: TelegramMessage,
    gateway: QGateway,
) -> None:
    """撤回 Telegram 编辑对应的 QQ 端旧消息，然后发送新消息。"""
    if not msg.replaced_q_message_ids:
        mapping = await _get_q_message(msg.group_id, msg.message_ids[0])
        if mapping is not None:
            msg.replaced_q_message_ids = mapping.q_message_ids
    for message_id in msg.replaced_q_message_ids:
        if message_id in msg.replacement_deleted_q_message_ids:
            continue
        try:
            suppress_onebot_recall(msg.q_group_id, message_id)
            await gateway.delete_message(message_id)
        except OneBotConnectionError:
            raise
        except OneBotResultUnknownError:
            raise
        except Exception as error:
            raise OneBotSendError from error
        msg.replacement_deleted_q_message_ids.add(message_id)
    msg.replacement_done = True


async def _save_telegram_message_mapping(msg: TelegramMessage) -> None:
    if msg.q_group_id is None:
        raise RuntimeError("OneBot 转发完成但缺少目标群 ID")
    await _save_or_queue_message_mapping(
        PendingMessageMapping(
            q_group_id=msg.q_group_id,
            q_message_ids=tuple(msg.q_message_ids),
            tg_chat_id=msg.group_id,
            tg_message_ids=msg.message_ids,
            tg_user_id=msg.user_id,
        ),
        error_message="OneBot 消息发送成功，但消息映射保存失败",
    )


async def _save_or_queue_message_mapping(
    mapping: PendingMessageMapping,
    *,
    error_message: str,
) -> None:
    try:
        await sql.set_message_mapping(**database_values(mapping))
    except Exception as database_error:
        try:
            await mapping_outbox.enqueue(mapping)
        except Exception as outbox_error:
            outbox_error.add_note(f"Database mapping write failed: {database_error!r}")
            raise MessageMappingError(error_message) from outbox_error
        baselog.exception("%s，已加入本地补偿队列", error_message)


async def _get_tg_message(q_group_id: int, q_message_id: int):
    pending_before = mapping_outbox.get_tg_message(q_group_id, q_message_id)
    try:
        mapping = await sql.get_tg_message(q_group_id, q_message_id)
    except Exception:
        pending = newest_pending_mapping(
            pending_before,
            mapping_outbox.get_tg_message(q_group_id, q_message_id),
        )
        if pending is None:
            raise
        return pending
    pending = newest_pending_mapping(
        pending_before,
        mapping_outbox.get_tg_message(q_group_id, q_message_id),
    )
    return newest_mapping(mapping, pending)


async def _get_q_message(tg_chat_id: int, tg_message_id: int):
    pending_before = mapping_outbox.get_q_message(tg_chat_id, tg_message_id)
    try:
        mapping = await sql.get_q_message(tg_chat_id, tg_message_id)
    except Exception:
        pending = newest_pending_mapping(
            pending_before,
            mapping_outbox.get_q_message(tg_chat_id, tg_message_id),
        )
        if pending is None:
            raise
        return pending
    pending = newest_pending_mapping(
        pending_before,
        mapping_outbox.get_q_message(tg_chat_id, tg_message_id),
    )
    return newest_mapping(mapping, pending)


async def forward_telegram_pin_to_onebot(
    tg_chat_id: int,
    tg_message_id: int,
    gateway: QGateway,
) -> None:
    """把 Telegram 置顶应用到映射中的全部 OneBot 消息。"""
    if not await sql.get_tg_forward_enabled(tg_chat_id):
        return
    mapping = await _get_q_message(tg_chat_id, tg_message_id)
    if mapping is None:
        baselog.warning(
            "Telegram 置顶事件没有可用的 OneBot 消息映射: chat=%s message=%s",
            tg_chat_id,
            tg_message_id,
        )
        return
    for message_id in mapping.q_message_ids:
        await gateway.set_essence_message(message_id)


def telegram_pin_task(
    tg_chat_id: int,
    tg_message_id: int,
    gateway: QGateway,
) -> SendTask:
    """创建发送到 OneBot 事件队列的 Telegram 置顶任务。"""
    return SendTask(
        target=SendTarget.ONEBOT,
        lane=SendLane.EVENT,
        send=partial(
            forward_telegram_pin_to_onebot,
            tg_chat_id,
            tg_message_id,
            gateway,
        ),
        failure_action=_onebot_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"telegram-pin:{tg_chat_id}:{tg_message_id}",
    )


async def forward_telegram_group_member_to_onebot(
    event: TelegramGroupMemberEvent,
    gateway: QGateway,
) -> None:
    """把 Telegram 群成员加入或退出事件格式化为 OneBot 文本消息。"""
    q_group_id = await sql.get_q_group(event.group_id)
    if q_group_id is None:
        baselog.warning("Telegram 群事件未配置转发目标: %s", event.group_id)
        return
    if not await sql.get_tg_forward_enabled(event.group_id):
        return
    action = "加入了群聊" if event.joined else "退出了群聊"
    text = "\n".join(f"{name} {action}" for name in event.member_names)
    try:
        await gateway.send_group_message(
            group_id=q_group_id,
            message=[{"type": "text", "data": {"text": text}}],
        )
    except OneBotConnectionError:
        raise
    except OneBotResultUnknownError:
        raise
    except Exception as error:
        raise OneBotSendError from error


def telegram_group_member_task(
    event: TelegramGroupMemberEvent,
    gateway: QGateway,
) -> SendTask:
    """创建发送到 OneBot 事件队列的 Telegram 群成员变动任务。"""
    action = "increase" if event.joined else "decrease"
    return SendTask(
        target=SendTarget.ONEBOT,
        lane=SendLane.EVENT,
        send=partial(forward_telegram_group_member_to_onebot, event, gateway),
        failure_action=_onebot_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        label=f"telegram-group-{action}:{event.group_id}",
    )


async def finalize_telegram_message(msg: TelegramMessage) -> None:
    """任务结束后归还队列预算，并关闭未被缓存接管的文件。"""
    if msg.replace_existing and not msg.replacement_finished:
        msg.replacement_finished = True
        finish_telegram_replacement(msg.group_id, msg.message_ids)
    if msg.queue_bytes:
        await media_queue_budget.release(msg.queue_bytes)
        msg.queue_bytes = 0
    if msg.media_ids is not None and msg.media_cache_pinned:
        media_cache.release_media_batch(msg.media_ids)
        msg.media_cache_pinned = False
    elif msg.media_ids is None:
        for attachment in msg.media:
            attachment.content.close()


def _is_mp4(content: MediaFile) -> bool:
    """Telegram 只有 MP4 能作为可播放视频，其它容器按文件发送。"""
    return _media_mime(content) == "video/mp4"


def _is_gif(content: MediaFile) -> bool:
    """通过文件签名识别 OneBot image 段中的动态 GIF。"""
    return _media_mime(content) == "image/gif"


def _media_mime(content: MediaFile) -> str | None:
    """读取公开格式的文件签名，不信任远端 MIME 和文件扩展名。"""
    position = content.file.tell()
    try:
        content.file.seek(0)
        kind = filetype.guess(content.file.read(261))
        return kind.mime if kind is not None else None
    finally:
        content.file.seek(position)


def _onebot_failure_action(error: Exception) -> FailureAction:
    """把 OneBot 连接状态和业务失败映射为通用总线动作。"""
    if isinstance(error, OneBotConnectionError):
        return FailureAction.DEFER
    if isinstance(error, OneBotResultUnknownError):
        return FailureAction.DROP
    if isinstance(error, OneBotSendError):
        return FailureAction.RETRY
    if isinstance(error, MessageMappingError):
        return FailureAction.RETRY
    return FailureAction.DROP


def _telegram_failure_action(error: Exception) -> FailureAction:
    """只重试结果明确未成功的错误，避免超时后重复发送。"""
    if isinstance(error, (MediaTooLargeError, NetworkError)):
        return FailureAction.DROP
    return FailureAction.RETRY


async def _disable_forwarding(
    msg: TelegramMessage,
    bot: Bot,
    gateway: QGateway,
    error: Exception,
) -> None:
    """向来源群提醒最终失败；业务发送耗尽时额外关闭转发。"""
    if isinstance(error, OneBotSendError):
        await sql.set_tg_forward_enabled(msg.group_id, False)
        q_group_id = await sql.get_q_group(msg.group_id)
        text = (
            "转发到 OneBot 连续失败 3 次，已自动关闭转发。"
            "请排查后使用 /forward on 重新开启。"
        )
        enqueue_bridge_notice(
            partial(bot.send_message, chat_id=msg.group_id, text=text),
            gateway,
            q_group_id=q_group_id,
            text=text,
        )
        return

    if isinstance(error, MessageMappingError):
        enqueue_telegram_notice(
            partial(
                bot.send_message,
                chat_id=msg.group_id,
                text="消息已发送到 OneBot，但消息映射保存失败，请检查数据库。",
            )
        )
        return

    if isinstance(error, OneBotResultUnknownError):
        enqueue_telegram_notice(
            partial(
                bot.send_message,
                chat_id=msg.group_id,
                text=(
                    "消息发送到 OneBot 的结果未知，为避免重复发送未自动重试，"
                    "请检查 OneBot 群。"
                ),
            )
        )
        return

    enqueue_telegram_notice(
        partial(
            bot.send_message,
            chat_id=msg.group_id,
            text="消息发送到 OneBot 失败，请稍后重试。",
        )
    )


async def _notify_onebot_telegram_failure(
    msg: OneBotMessage,
    gateway: QGateway,
    error: Exception,
) -> None:
    """Telegram 任务耗尽后只向来源 OneBot 群发送失败提示。"""
    if isinstance(error, MediaTooLargeError):
        emit_runtime_event("capability.succeeded", "onebot.media.rejected")
        text = str(error)
        label = f"onebot-media-rejected:{msg.group_id}:{msg.message_id}"
    elif isinstance(error, MessageMappingError):
        text = "消息已发送到 Telegram，但消息映射保存失败，请检查数据库。"
        label = f"onebot-mapping-failed:{msg.group_id}:{msg.message_id}"
    else:
        text = (
            "消息发送到 Telegram 超时，发送结果未知；为避免重复发送未自动重试，"
            "请检查 Telegram 群。"
            if isinstance(error, NetworkError)
            else "消息转发到 Telegram 连续失败 3 次，请稍后重试。"
        )
        label = f"onebot-forward-failed:{msg.group_id}:{msg.message_id}"
    enqueue_onebot_notice(
        gateway,
        q_group_id=msg.group_id,
        text=text,
        label=label,
    )


def telegram_forward_task(
    msg: TelegramMessage,
    gateway: QGateway,
    bot: Bot,
) -> SendTask:
    """创建目标为 OneBot、携带 OneBot 重试策略的通用发送任务。"""
    return SendTask(
        target=SendTarget.ONEBOT,
        send=partial(forward_telegram_to_onebot, msg, gateway),
        failure_action=_onebot_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        on_failed=partial(_disable_forwarding, msg, bot, gateway),
        finalize=partial(finalize_telegram_message, msg),
        label=f"telegram-to-onebot:{msg.group_id}:{msg.message_ids}",
    )


async def prepare_telegram_forward(
    msg: TelegramMessage,
    gateway: QGateway,
    bot: Bot,
) -> None:
    """串行规范化视频，完成后把发送任务交给 OneBot 队列。"""
    from src.bus import message_bus

    for attachment in msg.media:
        if attachment.processing == "video":
            await track_conversion(
                "video",
                normalize_video_for_onebot(
                    attachment.content,
                    size_limit=TELEGRAM_VIDEO_LIMIT,
                ),
            )
        elif attachment.processing == "sticker_static":
            await track_conversion(
                "sticker_static",
                static_sticker_to_png(
                    attachment.content,
                    size_limit=TELEGRAM_VIDEO_LIMIT,
                ),
            )
        elif attachment.processing == "sticker_tgs":
            await track_conversion(
                "sticker_tgs",
                tgs_sticker_to_gif(attachment.content),
            )
        elif attachment.processing == "sticker_video":
            await track_conversion(
                "sticker_video",
                video_sticker_to_gif(
                    attachment.content,
                    size_limit=TELEGRAM_VIDEO_LIMIT,
                ),
            )
    normalized_size = sum(attachment.content.size for attachment in msg.media)
    if normalized_size < msg.queue_bytes:
        await media_queue_budget.release(msg.queue_bytes - normalized_size)
        msg.queue_bytes = normalized_size
    await message_bus.put(telegram_forward_task(msg, gateway, bot))


async def _notify_processing_failure(
    msg: TelegramMessage,
    bot: Bot,
    error: Exception,
) -> None:
    """预处理已脱离 Update handler，失败时通过发送队列通知原 Telegram 群。"""
    enqueue_telegram_notice(
        partial(
            bot.send_message,
            chat_id=msg.group_id,
            text=f"媒体处理失败：{error}",
        )
    )


def telegram_processing_task(
    msg: TelegramMessage,
    gateway: QGateway,
    bot: Bot,
) -> ProcessingTask:
    """创建视频预处理任务；失败时释放尚未转交发送队列的媒体。"""
    return ProcessingTask(
        run=partial(prepare_telegram_forward, msg, gateway, bot),
        cleanup=partial(finalize_telegram_message, msg),
        on_error=partial(_notify_processing_failure, msg, bot),
        label=f"telegram-media:{msg.group_id}:{msg.message_ids}",
    )


def onebot_forward_task(
    msg: OneBotMessage,
    bot: ExtBot[None],
    client: httpx.AsyncClient,
    gateway: QGateway,
) -> SendTask:
    """创建目标为 Telegram、失败三次后仅通知 OneBot 的通用任务。"""
    return SendTask(
        target=SendTarget.TELEGRAM,
        send=partial(_forward_onebot_with_reply_fallback, msg, bot, client, gateway),
        failure_action=_telegram_failure_action,
        max_attempts=MAX_SEND_ATTEMPTS,
        on_failed=partial(_notify_onebot_telegram_failure, msg, gateway),
        finalize=partial(finalize_onebot_forward, msg, bot),
        label=f"onebot-to-telegram:{msg.group_id}:{msg.message_id}",
    )


async def _forward_onebot_with_reply_fallback(
    msg: OneBotMessage,
    bot: ExtBot[None],
    client: httpx.AsyncClient,
    gateway: QGateway,
) -> None:
    """回复目标已从 Telegram 删除时，改用明确的不可读引用文本。"""
    try:
        await forward_onebot_to_telegram(msg, bot, client, gateway)
    except BadRequest as error:
        message = str(error).lower()
        if (
            msg.reply_message_id is None
            or msg.reply_unavailable
            or msg.tg_message_ids
            or "repl" not in message
            or "not found" not in message
        ):
            raise
        baselog.warning(
            "Telegram 回复目标不存在，使用提示文本: group=%s message=%s reply=%s",
            msg.group_id,
            msg.message_id,
            msg.reply_message_id,
        )
        emit_runtime_event("capability.invalidated", "onebot.reply.mapped")
        emit_runtime_event("capability.succeeded", "onebot.reply.unavailable")
        msg.reply_message_id = None
        msg.reply_unavailable = True
        await forward_onebot_to_telegram(msg, bot, client, gateway)

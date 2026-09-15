from __future__ import annotations

"""平台无关的内部消息与转发任务模型。"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Literal
from urllib.parse import urlsplit

from src.media import MediaFile

ONEBOT_USER_NAME = "OneBot 用户"
ONEBOT_SEND_FAILED_TEXT = "消息发送到 OneBot 失败，请稍后重试。"


def is_http_url(value: str) -> bool:
    """校验带主机名的 HTTP(S) URL。"""
    parsed = urlsplit(value.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def onebot_user_id(value: Any) -> int | None:
    """解析 OneBot 用户 ID，拒绝布尔值和非数字文本。"""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def onebot_user_name(data: Mapping[str, Any]) -> str | None:
    """按群名片、昵称顺序取得非空 OneBot 用户名称。"""
    for key in ("card", "nickname"):
        value = data.get(key)
        if isinstance(value, str) and (value := value.strip()):
            return value
    return None


@dataclass(frozen=True, slots=True)
class TelegraphPageRef:
    title: str
    url: str


@dataclass(slots=True, kw_only=True)
class OneBotMessage:
    """经过入口校验的 OneBot 群消息。"""

    message_id: int
    group_id: int
    user_id: int
    sender_name: str
    message: list[dict[Any, Any]]
    self_id: int | None = None
    sender_name_is_fallback: bool = False
    reply_message_id: int | None = None
    reply_unavailable: bool = False
    # Telegram 发送可能在多媒体中途失败。进度保存在任务 DTO 中，使重试不会
    # 重复发送已经成功的前序媒体。
    tg_message_ids: list[int] = field(default_factory=list)
    next_media_index: int = 0
    next_text_chunk_index: int = 0
    # 群成员名称查询结果跨发送重试复用；None 表示查询失败。
    mention_names: dict[int, str | None] = field(default_factory=dict)
    # 转发开始后记录实际 Telegram 目标，供并发到达的撤回事件清理部分发送结果。
    tg_chat_id: int | None = None
    # Telegraph 页面创建后跨 Telegram 发送重试复用，避免生成重复页面。
    telegraph_pages: dict[str, TelegraphPageRef] = field(default_factory=dict)
    # 群公告文件名跨 Telegram 发送重试复用，避免每次尝试生成不同名称。
    announcement_filename: str | None = None
    # 远端发送完成后，映射写入失败的重试只能再次写库，不能重复发送。
    tg_forward_complete: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class OneBotGroupBanEvent:
    """经过入口校验的 OneBot 群禁言或解除禁言事件。"""

    group_id: int
    operator_id: int
    user_id: int
    duration: int
    lifted: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class OneBotGroupMemberEvent:
    """经过入口校验的 OneBot 群成员加入或退出事件。"""

    group_id: int
    user_id: int
    joined: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class TelegramGroupMemberEvent:
    """经过入口校验的 Telegram 群成员加入或退出事件。"""

    group_id: int
    member_names: tuple[str, ...]
    joined: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class OneBotPokeEvent:
    """经过入口校验的 OneBot 群戳一戳事件。"""

    group_id: int
    user_id: int
    target_id: int
    action: str
    suffix: str


@dataclass(frozen=True, slots=True, kw_only=True)
class OneBotEssenceEvent:
    """经过入口校验的 OneBot 精华消息添加或删除事件。"""

    group_id: int
    message_id: int
    added: bool


@dataclass(slots=True, kw_only=True)
class TelegramMedia:
    """Telegram 入站媒体及其对应的 OneBot 消息段类型。"""

    kind: Literal["file", "image", "record", "video"]
    content: MediaFile
    processing: Literal[
        "none",
        "video",
        "sticker_static",
        "sticker_tgs",
        "sticker_video",
    ] = "none"


@dataclass(slots=True, kw_only=True)
class TelegramMessage:
    """Telegram 文本、单媒体或已聚合媒体组。"""

    message_ids: tuple[int, ...]
    group_id: int
    user_id: int
    sender_name: str
    text: str | None
    at_user_id: int | None = None
    bot_forward_required: bool = False
    forwarded_from: str | None = None
    reply_message_id: int | None = None
    media: tuple[TelegramMedia, ...] = ()
    media_ids: tuple[str, ...] | None = None
    media_cache_pinned: bool = False
    queue_bytes: int = 0
    q_message_ids: list[int] = field(default_factory=list)
    next_onebot_batch: int = 0
    q_group_id: int | None = None
    # Telegram edited messages cannot be edited through OneBot.  The old QQ
    # message is recalled first and this message is sent as a fresh message;
    # saving the new mapping makes later Telegram replies target the new ID.
    replace_existing: bool = False
    replacement_done: bool = False
    replacement_finished: bool = False
    replaced_q_message_ids: tuple[int, ...] = ()
    replacement_deleted_q_message_ids: set[int] = field(default_factory=set)
    q_forward_complete: bool = False


class SendTarget(Enum):
    """发送任务的目标平台。"""

    ONEBOT = auto()
    TELEGRAM = auto()


class SendLane(Enum):
    """按桥接领域语义隔离同一目标平台的发送任务。"""

    MESSAGE = auto()
    EVENT = auto()
    SYSTEM = auto()


class FailureAction(Enum):
    """任务失败后由通用总线执行的动作。"""

    DROP = auto()
    RETRY = auto()
    DEFER = auto()


def drop_failure(error: Exception) -> FailureAction:
    """默认策略：记录单次失败，不重试。"""
    return FailureAction.DROP


@dataclass(slots=True, kw_only=True)
class SendTask:
    """平台无关的可发送任务及其失败、耗尽和资源清理策略。"""

    target: SendTarget
    send: Callable[[], Awaitable[None]]
    lane: SendLane = SendLane.MESSAGE
    failure_action: Callable[[Exception], FailureAction] = drop_failure
    max_attempts: int = 1
    failures: int = 0
    on_failed: Callable[[Exception], Awaitable[None]] | None = None
    finalize: Callable[[], Awaitable[None]] | None = None
    label: str = "send-task"


class OneBotSendError(Exception):
    """OneBot 在线时 send_group_msg 返回业务失败。"""


class OneBotConnectionError(Exception):
    """SnowLuma WebSocket 不可用于发送。"""


class OneBotResultUnknownError(Exception):
    """OneBot action 已开始，但响应丢失，执行结果未知。"""


class SendTargetUnavailableError(Exception):
    """发送目标在生产者等待队列空间期间停止。"""


class MessageMappingError(Exception):
    """远端发送完成后，本地消息映射保存失败。"""


class MediaTooLargeError(Exception):
    """媒体超过桥接服务允许转发的大小。"""

"""临时媒体文件、读取租约以及文件数量/字节数背压工具。"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from secrets import token_urlsafe
from tempfile import NamedTemporaryFile, SpooledTemporaryFile
from typing import Any

from src.lifecycle import await_completion_on_cancel
from src.paths import ensure_temp_dir

# Telegram Bot API 的云端 getFile 下载上限与媒体上传上限不同，必须分别维护。
TELEGRAM_DOWNLOAD_LIMIT = 20_000_000
TELEGRAM_DOWNLOAD_LIMIT_TEXT = f"{TELEGRAM_DOWNLOAD_LIMIT // 1_000_000} MB"
TELEGRAM_UPLOAD_LIMIT = 50_000_000
TELEGRAM_UPLOAD_LIMIT_TEXT = f"{TELEGRAM_UPLOAD_LIMIT // 1_000_000} MB"
# 所有 ffmpeg 调用共用的基础参数：禁用交互、隐藏 banner、只输出错误、覆盖输出。
FFMPEG_BASE_ARGS = ("ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y")
# 子进程 stderr 解码后保留的末尾字节数，避免超长日志淹没错误信息。
PROCESS_ERROR_TAIL = 500

SPOOL_MEMORY_LIMIT = 1 * 1024 * 1024
# 1 MiB 以内的媒体无条件留在内存；1 MiB 到 MEDIA_MEMORY_TIER_LIMIT 之间的媒体
# 需要先取得内存额度才留在内存，超过则直接落盘。低内存 VPS 上单纯放大
# SPOOL_MEMORY_LIMIT 会让最坏内存占用等于 MEDIA_RESOURCE_LIMIT 乘以该阈值，
# 因此中间档必须配额化。
MEDIA_MEMORY_TIER_LIMIT = 5 * 1024 * 1024
MEDIA_MEMORY_BUDGET = 64 * 1024 * 1024
# 落盘后的 SpooledTemporaryFile 会持续占用一个文件描述符。这里限制的不只是
# Python 对象数量，也是在给系统的 open files 上限留出余量。
MEDIA_RESOURCE_LIMIT = 256
MEDIA_QUEUE_MAX_BYTES = 512 * 1024 * 1024
MEDIA_CACHE_MAX_BYTES = 512 * 1024 * 1024
MEDIA_CACHE_TTL = 120
STREAM_CHUNK_SIZE = 256 * 1024


async def start_media_process(
    *args: str,
    missing_error: str,
    **kwargs: Any,
) -> asyncio.subprocess.Process:
    """启动媒体子进程，并隐藏可执行文件缺失的底层路径信息。"""
    try:
        return await asyncio.create_subprocess_exec(
            *args,
            stderr=asyncio.subprocess.PIPE,
            **kwargs,
        )
    except FileNotFoundError:
        raise ValueError(missing_error) from None


async def communicate_media_process(
    process: asyncio.subprocess.Process,
    *,
    timeout: int,
    timeout_error: str,
) -> tuple[bytes, bytes]:
    """等待媒体子进程，并在超时、取消或异常时确保进程已回收。"""
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise ValueError(timeout_error) from None
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    return stdout or b"", stderr or b""


async def run_media_thread[T](function: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """在线程操作真正结束后再向上传播外层任务取消。"""
    return await await_completion_on_cancel(
        asyncio.to_thread(function, *args, **kwargs)
    )


@asynccontextmanager
async def media_input_path(
    media: MediaFile,
    *,
    suffix: str = ".media",
) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    """为媒体子进程提供一个可读取的输入路径及其进程参数。

    Linux 上使用继承给 ffmpeg/ffprobe 的文件描述符，避免复制媒体内容。Windows
    没有 ``/proc/self/fd``，而且 ``pass_fds`` 不受 asyncio 子进程支持，因此先把
    内容复制到一个已关闭的临时文件，再把普通路径传给子进程。
    """
    if os.name != "nt":
        input_fd = media.fileno()
        media.rewind()
        yield f"/proc/self/fd/{input_fd}", {"pass_fds": (input_fd,)}
        return

    with NamedTemporaryFile(
        suffix=suffix,
        delete=False,
        dir=str(ensure_temp_dir()),
    ) as source:
        input_path = Path(source.name)
    try:
        await run_media_thread(_copy_media_to_path, media, input_path)
        yield str(input_path), {}
    finally:
        input_path.unlink(missing_ok=True)


def _copy_media_to_path(media: MediaFile, output_path: Path) -> None:
    """将 MediaFile 复制到一个可由 Windows 子进程重新打开的路径。"""
    media.rewind()
    with output_path.open("wb") as output:
        while chunk := media.file.read(STREAM_CHUNK_SIZE):
            output.write(chunk)
    media.rewind()


def replace_media_content(media: MediaFile, output_path: Path) -> None:
    """用临时输出文件分块覆盖 MediaFile。

    转码产物的大小与申请内存额度时的声明值无关，一律不参与内存档：退出内存档
    归还额度并落盘，再覆盖内容，避免额度计数随转码结果漂移。

    清空要放在退出内存档之前：rollover() 会把当前缓冲区完整拷进磁盘文件，而旧
    内容马上就要被覆盖，先 truncate 可以省掉这次无用拷贝。
    """
    media.file.seek(0)
    media.file.truncate()
    media.size = 0
    media.leave_memory_tier()
    with output_path.open("rb") as converted:
        while chunk := converted.read(STREAM_CHUNK_SIZE):
            media.write(chunk)
    media.rewind()


def decode_process_error(stderr: bytes) -> str:
    """解码子进程 stderr，只保留末尾片段，避免超长日志淹没错误信息。"""
    error = stderr.decode("utf-8", errors="replace").strip()
    return error[-PROCESS_ERROR_TAIL:] if error else "未知错误"


@asynccontextmanager
async def transcode_target(suffix: str) -> AsyncIterator[Path]:
    """在临时目录创建转码输出文件，并在结束时无条件删除。"""
    with NamedTemporaryFile(suffix=suffix, delete=False, dir=str(ensure_temp_dir())) as output:
        output_path = Path(output.name)
    try:
        yield output_path
    finally:
        output_path.unlink(missing_ok=True)


def finalize_media(
    media: MediaFile,
    output_path: Path,
    *,
    stem_fallback: str,
    suffix: str,
    media_type: str,
) -> None:
    """用转码产物覆盖 MediaFile，并统一设置文件名与媒体类型。

    内部是同步操作；需要避免阻塞事件循环时，调用方应使用 asyncio.to_thread 包裹。
    """
    replace_media_content(media, output_path)
    media.filename = f"{Path(media.filename).stem or stem_fallback}{suffix}"
    media.media_type = media_type


class ByteBudget:
    """按总字节数提供背压。

    asyncio.Queue(maxsize=100) 只能限制消息条数，但一条消息可能是几字节文本，
    也可能是包含十张图片的相册。生产者在下载图片前先 acquire，消费者处理
    完事后 release，这样排队媒体的总大小达到上限时，生产者会等待而不是
    继续占用内存或临时磁盘。
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0
        self._condition = asyncio.Condition()

    async def acquire(self, size: int) -> None:
        if size > self.limit:
            raise ValueError(f"资源大小 {size} 超过预算上限 {self.limit}")
        async with self._condition:
            # Condition 会释放锁并休眠；release() 通知后再重新检查条件。
            await self._condition.wait_for(lambda: self.used + size <= self.limit)
            self.used += size

    async def release(self, size: int) -> None:
        async with self._condition:
            self.used -= size
            if self.used < 0:
                raise RuntimeError("媒体队列预算计数错误")
            self._condition.notify_all()


class ItemBudget:
    """限制同时存在的 MediaFile 数量。

    大文件 rollover 到磁盘后，每个 MediaFile 都持有一个打开的临时文件。
    即使总字节数未超限，极多小文件也可能先耗尽系统文件描述符，因此项目
    同时使用“字节预算”和“文件数量预算”。
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0
        self._waiters: list[asyncio.Future[None]] = []

    async def acquire(self, count: int = 1, *, reserve: int = 0) -> None:
        # reserve 给消费者即时下载留槽，避免队列占满全部槽位后互相等待。
        available = self.limit - reserve
        if count > available:
            raise ValueError(f"资源数量 {count} 超过预算上限 {self.limit}")
        while self.used + count > available:
            # ItemBudget.release() 是同步方法，因此这里使用 Future 作为等待信号，
            # 不要求关闭文件的调用方为了归还一个计数而变成异步函数。
            waiter = asyncio.get_running_loop().create_future()
            self._waiters.append(waiter)
            try:
                await waiter
            finally:
                self._waiters.remove(waiter)
        self.used += count

    def release(self, count: int = 1) -> None:
        self.used -= count
        if self.used < 0:
            raise RuntimeError("媒体资源数量计数错误")
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)


class MemoryBudget:
    """限制中间档媒体驻留内存的总字节数。

    与 ByteBudget 不同，这里的语义是“拿不到额度就落盘”，调用方永不等待，
    因此不需要 Condition，也不会与 ByteBudget、ItemBudget 叠加出新的等待环。

    但 release() 会经 asyncio.to_thread 在转码 worker 线程被调用
    （replace_media_content 走 sticker/video 的 to_thread 路径），而
    try_acquire() 在事件循环线程执行，两者对 self.used 构成真正的跨线程读改
    写。self.used += size / -= size 会编译成 LOAD/运算/STORE 多条字节码，GIL
    在字节码之间切换线程即可丢失更新，因此这里用 threading.Lock 保护，而不是
    依赖 GIL 的原子性假设。锁临界区只有整数加减，无竞争获取是纳秒级开销。
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0
        self._lock = threading.Lock()

    def try_acquire(self, size: int) -> bool:
        with self._lock:
            if self.used + size > self.limit:
                return False
            self.used += size
            return True

    def release(self, size: int) -> None:
        with self._lock:
            self.used -= size
            if self.used < 0:
                raise RuntimeError("媒体内存额度计数错误")


media_item_budget = ItemBudget(MEDIA_RESOURCE_LIMIT)
media_queue_budget = ByteBudget(MEDIA_QUEUE_MAX_BYTES)
media_memory_budget = MemoryBudget(MEDIA_MEMORY_BUDGET)


def _acquire_memory_tier(expected_size: int | None) -> int:
    """按声明大小决定内存驻留额度，返回实际占用的字节数。

    声明大小未知时返回 0，调用方沿用 SPOOL_MEMORY_LIMIT 老路径：此时既不预留
    额度，也不改变行为，避免把没有 Content-Length 的小文件反而逼去落盘。
    """
    if expected_size is None or expected_size <= SPOOL_MEMORY_LIMIT:
        return 0
    if expected_size > MEDIA_MEMORY_TIER_LIMIT:
        return 0
    return expected_size if media_memory_budget.try_acquire(expected_size) else 0


def _create_spool(
    expected_size: int | None,
    memory_reserved: int,
) -> SpooledTemporaryFile[bytes]:
    """按分档结果创建 spool，必要时立即落盘。

    注意 max_size=0 在 SpooledTemporaryFile 里表示“永不 rollover”而不是“立即
    落盘”：它的 _check 写作 ``if max_size and file.tell() > max_size``，0 是
    假值。因此声明大小已经超出免费档又没拿到额度时，必须显式调用 rollover()，
    省掉先在内存里堆满再触发 rollover 的那一次多余拷贝。

    取得额度时 max_size 必须等于收取的额度本身，而不是 MEDIA_MEMORY_TIER_LIMIT：
    额度是按声明大小收的，若允许驻留到档位上限，声明 1 MiB 的文件就能实际占用
    5 MiB 内存，整个额度池会被放大到设计值的数倍。
    """
    file = SpooledTemporaryFile(
        max_size=memory_reserved if memory_reserved else SPOOL_MEMORY_LIMIT,
        mode="w+b",
        dir=str(ensure_temp_dir()),
    )
    if not memory_reserved and expected_size is not None and expected_size > SPOOL_MEMORY_LIMIT:
        try:
            file.rollover()
        except BaseException:
            file.close()
            raise
    return file


class MediaFile:
    """一个可在内存与临时磁盘之间自动切换的媒体文件。

    默认不超过 SPOOL_MEMORY_LIMIT 时由 SpooledTemporaryFile 保存在内存；超过
    后自动 rollover 到系统临时目录。对象关闭时，底层临时文件会自动删除。
    创建时若声明了 expected_size，则按 _acquire_memory_tier 的分档结果决定是否
    用 media_memory_budget 的额度把中间档文件也留在内存。

    MediaFile 的所有权必须明确：创建者负责关闭；进入 TelegramMessage 后由任务负责；
    成功加入 MediaCache 后改由媒体缓存负责。读取租约用于保证 HTTP 响应尚未发送完时，
    TTL 清理只把文件标记为待关闭，不会中途截断响应。
    """

    def __init__(
        self,
        file: SpooledTemporaryFile[bytes],
        *,
        filename: str,
        media_type: str,
        owns_item_slot: bool,
        memory_reserved: int = 0,
    ) -> None:
        self.file = file
        self.filename = filename
        self.media_type = media_type
        self.size = 0
        self._lock = asyncio.Lock()
        self._closed = False
        self._close_pending = False
        self._leases = 0
        self._owns_item_slot = owns_item_slot
        self._memory_reserved = memory_reserved
        self._close_callbacks: list[Callable[[], None]] = []

    @classmethod
    async def create(
        cls,
        *,
        filename: str = "image",
        media_type: str = "application/octet-stream",
        expected_size: int | None = None,
    ) -> MediaFile:
        # 普通创建路径自行申请一个全局文件名额。
        await media_item_budget.acquire()
        memory_reserved = _acquire_memory_tier(expected_size)
        try:
            file = _create_spool(expected_size, memory_reserved)
        except BaseException:
            if memory_reserved:
                media_memory_budget.release(memory_reserved)
            media_item_budget.release()
            raise
        return cls(
            file,
            filename=filename,
            media_type=media_type,
            owns_item_slot=True,
            memory_reserved=memory_reserved,
        )

    @classmethod
    def create_reserved(
        cls,
        *,
        filename: str = "image",
        media_type: str = "application/octet-stream",
        expected_size: int | None = None,
    ) -> MediaFile:
        # 调用方已批量取得 ItemBudget；这里不能再次 acquire。内存额度是同步的
        # try_acquire，拿不到就落盘，所以同步方法也能参与分档。
        memory_reserved = _acquire_memory_tier(expected_size)
        try:
            file = _create_spool(expected_size, memory_reserved)
        except BaseException:
            if memory_reserved:
                media_memory_budget.release(memory_reserved)
            raise
        return cls(
            file,
            filename=filename,
            media_type=media_type,
            owns_item_slot=True,
            memory_reserved=memory_reserved,
        )

    def leave_memory_tier(self) -> None:
        """让文件退出内存档：确保已落盘并归还额度。

        转码产物大小与申请额度时的声明值无关，取 fileno() 也会强制 spool 落盘，
        这些路径都必须先归还额度，否则计数会一直挂在已经落盘的文件上。
        """
        if not self._memory_reserved:
            return
        reserved = self._memory_reserved
        if not self._closed:
            self.file.rollover()
        # rollover 失败时保留额度归属，后续 close 仍能正确归还。
        self._memory_reserved = 0
        media_memory_budget.release(reserved)

    def fileno(self) -> int:
        """返回底层文件描述符。

        SpooledTemporaryFile.fileno() 会强制 rollover，因此这里必须先退出内存
        档。子进程需要真实 fd 的调用方都应该走这个方法，而不是 file.fileno()。
        """
        if self._closed:
            raise ValueError("媒体文件已关闭")
        self.leave_memory_tier()
        return self.file.fileno()

    def write(self, chunk: bytes) -> None:
        if self._closed:
            raise ValueError("媒体文件已关闭")
        self.file.write(chunk)
        # 声明大小可能偏小。额度按声明值收取，spool 的 max_size 也等于该额度，
        # 因此写入超过额度时 spool 已经自动 rollover，这里同步归还额度，避免
        # 额度长期挂在已落盘的文件上。
        if self._memory_reserved and self.size + len(chunk) > self._memory_reserved:
            self.leave_memory_tier()
        # SpooledTemporaryFile 没有直接暴露业务需要的稳定长度属性，所以写入时
        # 自己累计；下载上限、队列预算和 Content-Length 都使用这个值。
        self.size += len(chunk)

    def rewind(self) -> None:
        if self._closed:
            raise ValueError("媒体文件已关闭")
        self.file.seek(0)

    def chunks(self) -> MediaStream:
        if self._closed or self._close_pending:
            raise ValueError("媒体文件已关闭")
        # 每个 HTTP 响应取得一个租约。即使还未读取第一个 chunk，响应取消时
        # MediaResponse 也会调用 aclose() 归还它。
        self._leases += 1
        return MediaStream(self)

    def add_close_callback(self, callback: Callable[[], None]) -> None:
        # MediaCache 用回调在文件“真正关闭”时归还字节容量；不能在 URL 过期时提前
        # 归还，因为此时可能仍有一个已经开始的 HTTP 响应持有该文件。
        if self._closed:
            callback()
            return
        self._close_callbacks.append(callback)

    def close(self) -> None:
        # close 可以由多个异常清理路径调用，所以必须保持幂等。
        if self._closed or self._close_pending:
            return
        self._close_pending = True
        # TTL 可以先使 URL 失效，但正在发送的响应仍需读完底层文件。
        if self._leases == 0:
            self._finish_close()

    def _finish_close(self) -> None:
        # 只有没有活跃读取租约时才会进入这里。关闭文件会删除已落盘的临时文件。
        self._closed = True
        self.file.close()
        # 额度必须在文件真正关闭时归还，而不是 close() 被调用时：此前可能仍有
        # 活跃 HTTP 响应持有该文件，内存也仍未释放。
        if self._memory_reserved:
            reserved = self._memory_reserved
            self._memory_reserved = 0
            media_memory_budget.release(reserved)
        for callback in self._close_callbacks:
            callback()
        self._close_callbacks.clear()
        if self._owns_item_slot:
            self._owns_item_slot = False
            media_item_budget.release()


class MediaStream(AsyncIterator[bytes]):
    """MediaFile 的单次异步读取会话。

    一个临时 URL 在 TTL 内可以被 SnowLuma 重试，因此同一文件可能被请求多次。
    所有请求共享同一个文件游标，必须用锁串行执行 seek/read，否则两个响应会
    相互移动游标并得到损坏的内容。这里的文件读取本身是同步操作，但每次最多
    STREAM_CHUNK_SIZE，避免一次把整张图片重新读入内存。
    """

    def __init__(self, media: MediaFile) -> None:
        self._media = media
        self._started = False
        self._closed = False

    def __aiter__(self) -> MediaStream:
        return self

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration
        if not self._started:
            # 锁要持有到整个响应读取结束，而不是每个 chunk 单独加锁，否则两个
            # 响应仍可能在 chunk 之间交错并改变共享游标。
            await self._media._lock.acquire()
            self._media.rewind()
            self._started = True
        chunk = self._media.file.read(STREAM_CHUNK_SIZE)
        if chunk:
            return chunk
        await self.aclose()
        raise StopAsyncIteration

    async def aclose(self) -> None:
        # 正常读完、客户端断开和 ASGI 发送失败最终都会走到这里。
        if self._closed:
            return
        self._closed = True
        if self._started:
            self._media._lock.release()
        self._media._leases -= 1
        if self._media._close_pending and self._media._leases == 0:
            self._media._finish_close()


@dataclass(slots=True)
class CachedMedia:
    """临时媒体文件、URL 过期时间及待发送任务持有的租约数。"""

    content: MediaFile
    expires_at: float
    pins: int = 0


class MediaCache:
    """管理 Telegram 图片的短期 HTTP 下载映射和文件所有权。"""

    def __init__(self) -> None:
        self._media: dict[str, CachedMedia] = {}
        self._media_bytes = 0

    def set_media_batch(
        self,
        contents: Sequence[MediaFile],
        *,
        pinned: bool = False,
    ) -> tuple[str, ...]:
        """整批缓存图片并返回临时 URL ID，成功后接管所有文件。"""
        self.purge_expired()
        batch_size = sum(content.size for content in contents)
        if self._media_bytes + batch_size > MEDIA_CACHE_MAX_BYTES:
            raise ValueError("临时媒体缓存超过 512 MiB 上限")

        expires_at = time.time() + MEDIA_CACHE_TTL
        media_ids = tuple(token_urlsafe(32) for _ in contents)
        for media_id, content in zip(media_ids, contents, strict=True):
            # 活跃 HTTP 响应会延迟关闭文件，容量也必须在真正关闭时才归还。
            content.add_close_callback(lambda size=content.size: self._release_bytes(size))
            self._media[media_id] = CachedMedia(
                content=content,
                expires_at=expires_at,
                pins=int(pinned),
            )
        self._media_bytes += batch_size
        return media_ids

    def get_media(self, media_id: str) -> MediaFile | None:
        """取得未过期媒体；命中过期项时立即使 URL 失效。"""
        media = self._media.get(media_id)
        if media is None:
            return None
        if media.pins == 0 and media.expires_at <= time.time():
            self._remove(media_id, media)
            return None
        return media.content

    def purge_expired(self) -> None:
        """使所有过期 URL 失效，并请求安全关闭对应临时文件。"""
        now = time.time()
        for media_id, media in list(self._media.items()):
            if media.pins == 0 and media.expires_at <= now:
                self._remove(media_id, media)

    def release_media_batch(self, media_ids: Sequence[str]) -> None:
        """释放发送任务租约，并从释放时刻开始计算 URL 的 TTL。"""
        expires_at = time.time() + MEDIA_CACHE_TTL
        for media_id in media_ids:
            media = self._media.get(media_id)
            if media is None:
                continue
            if media.pins <= 0:
                raise RuntimeError("临时媒体缓存租约计数错误")
            media.pins -= 1
            if media.pins == 0:
                media.expires_at = expires_at

    def close(self) -> None:
        """使全部 URL 失效，并请求安全关闭缓存持有的文件。"""
        for media_id, media in list(self._media.items()):
            self._remove(media_id, media)

    def _remove(self, media_id: str, media: CachedMedia) -> None:
        if self._media.pop(media_id, None) is None:
            return
        media.content.close()

    def _release_bytes(self, size: int) -> None:
        # 统计值包含过期但仍被活跃 HTTP 响应读取的文件。
        self._media_bytes -= size


media_cache = MediaCache()

"""将 Telegram 原生贴纸转换为 OneBot 可显示的图片。"""

import asyncio
import gzip
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from PIL import Image, UnidentifiedImageError

from src.log import baselog
from src.media import (
    FFMPEG_BASE_ARGS,
    STREAM_CHUNK_SIZE,
    TELEGRAM_DOWNLOAD_LIMIT_TEXT,
    MediaFile,
    communicate_media_process,
    decode_process_error,
    finalize_media,
    media_input_path,
    run_media_thread,
    start_media_process,
    transcode_target,
)
from src.paths import ensure_temp_dir
from src.runtime_events import emit_runtime_event

STICKER_TRANSCODE_TIMEOUT = 120
TGS_JSON_SIZE_LIMIT = 5_000_000
CONTAINER_MARKER = Path("/app/.q2tg-container")
LOTTIE_CONVERTER_IMAGE = (
    "edasriyan/lottie-to-gif@"
    "sha256:0eb24cf4f38c6c62b66f37bfba463fff4de4f64cb9a6127df0b9543fc4b9c649"
)


async def static_sticker_to_png(media: MediaFile, *, size_limit: int) -> None:
    """使用 Pillow 将静态贴纸或 TGS 缩略图转换为透明 PNG。"""
    started_at = time.monotonic()
    async with transcode_target(".png") as output_path:
        try:
            await run_media_thread(_render_png, media, output_path)
        except (OSError, UnidentifiedImageError):
            raise ValueError("Telegram 静态贴纸转换失败") from None
        if output_path.stat().st_size > size_limit:
            raise ValueError(
                f"Telegram 贴纸转换后超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT} 上限"
            )
        await run_media_thread(
            finalize_media,
            media,
            output_path,
            stem_fallback="sticker",
            suffix=".png",
            media_type="image/png",
        )
        emit_runtime_event("capability.succeeded", "telegram.sticker.static")
        baselog.info("静态贴纸转换完成，耗时 %.2f 秒", time.monotonic() - started_at)


async def video_sticker_to_gif(media: MediaFile, *, size_limit: int) -> None:
    """使用 ffmpeg 将 WebM/VP9 视频贴纸转换为循环透明 GIF。"""
    started_at = time.monotonic()
    async with transcode_target(".gif") as output_path:
        async with media_input_path(media, suffix=".webm") as (
            input_path,
            process_kwargs,
        ):
            process = await start_media_process(
                *FFMPEG_BASE_ARGS,
                "-c:v",
                "libvpx-vp9",
                "-i",
                input_path,
                "-filter_complex",
                (
                    "[0:v]fps=15,scale=512:512:force_original_aspect_ratio=decrease:"
                    "flags=lanczos,split[s0][s1];"
                    "[s0]palettegen=reserve_transparent=1:stats_mode=diff[p];"
                    "[s1][p]paletteuse=dither=sierra2_4a:alpha_threshold=128"
                ),
                "-loop",
                "0",
                "-fs",
                str(size_limit + 1),
                str(output_path),
                **process_kwargs,
                missing_error="视频贴纸转发需要安装 ffmpeg",
            )
            _, stderr = await communicate_media_process(
                process,
                timeout=STICKER_TRANSCODE_TIMEOUT,
                timeout_error="Telegram 视频贴纸转换超时",
            )
        if process.returncode != 0:
            raise ValueError(f"Telegram 视频贴纸转换失败: {decode_process_error(stderr)}")
        if output_path.stat().st_size > size_limit:
            raise ValueError(
                f"Telegram 贴纸转换后超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT} 上限"
            )
        await run_media_thread(
            finalize_media,
            media,
            output_path,
            stem_fallback="sticker",
            suffix=".gif",
            media_type="image/gif",
        )
        emit_runtime_event("capability.succeeded", "telegram.sticker.video")
        baselog.info("视频贴纸转码完成，耗时 %.2f 秒", time.monotonic() - started_at)


async def tgs_sticker_to_gif(media: MediaFile) -> None:
    """使用 lottie-converter 将 TGS 转换为循环透明 GIF。"""
    started_at = time.monotonic()
    temp_dir = str(ensure_temp_dir())
    with TemporaryDirectory(dir=temp_dir) as conversion_dir:
        source_path = Path(conversion_dir) / "sticker.json"
        output_path = Path(conversion_dir) / "sticker.gif"
        try:
            media.rewind()
            with gzip.GzipFile(fileobj=media.file, mode="rb") as compressed:
                _copy_tgs_json(compressed, source_path)
        except (gzip.BadGzipFile, EOFError, OSError):
            raise ValueError("Telegram TGS 贴纸解压失败") from None
        finally:
            media.rewind()

        if CONTAINER_MARKER.is_file():
            await _run_sticker_process(
                "bash",
                "/usr/local/bin/lottie_to_gif.sh",
                *_lottie_arguments(source_path, output_path),
                missing_error="TGS 贴纸转发需要安装 bash 和 lottie-converter",
                failure_prefix="Telegram TGS 贴纸转换失败",
            )
        else:
            await _run_sticker_process(
                "docker",
                "info",
                "--format",
                "{{.ServerVersion}}",
                missing_error="本地 TGS 贴纸转发需要安装 Docker",
                failure_prefix="Docker daemon 不可用或当前用户无访问权限",
            )
            mount = source_path.parent.resolve()
            await _run_sticker_process(
                "docker",
                "run",
                "--rm",
                *_docker_user_arguments(),
                "--volume",
                f"{mount}:/source",
                LOTTIE_CONVERTER_IMAGE,
                "bash",
                "/usr/bin/lottie_to_gif.sh",
                *_lottie_arguments(
                    f"/source/{source_path.name}",
                    f"/source/{output_path.name}",
                ),
                missing_error="本地 TGS 贴纸转发需要安装 Docker",
                failure_prefix="Docker TGS 贴纸转换失败",
            )
        await run_media_thread(
            finalize_media,
            media,
            output_path,
            stem_fallback="sticker",
            suffix=".gif",
            media_type="image/gif",
        )
        emit_runtime_event("capability.succeeded", "telegram.sticker.tgs")
        baselog.info("TGS 贴纸转码完成，耗时 %.2f 秒", time.monotonic() - started_at)


def _lottie_arguments(source_path: str | Path, output_path: str | Path) -> tuple[str, ...]:
    return (
        "--width",
        "512",
        "--height",
        "512",
        "--fps",
        "30",
        "--quality",
        "90",
        "--threads",
        "1",
        "--output",
        str(output_path),
        str(source_path),
    )


def _docker_user_arguments() -> tuple[str, ...]:
    """在 Unix 上保留宿主用户映射；Windows 没有 getuid/getgid。"""
    if hasattr(os, "getuid") and hasattr(os, "getgid"):
        return ("--user", f"{os.getuid()}:{os.getgid()}")
    return ()


def _copy_tgs_json(compressed: gzip.GzipFile, output_path: Path) -> None:
    """限制解压体积，避免畸形 TGS 过度占用临时存储。"""
    written = 0
    with output_path.open("wb") as output:
        while chunk := compressed.read(STREAM_CHUNK_SIZE):
            written += len(chunk)
            if written > TGS_JSON_SIZE_LIMIT:
                raise ValueError("Telegram TGS 贴纸解压后过大")
            output.write(chunk)


async def _run_sticker_process(
    *args: str,
    missing_error: str,
    failure_prefix: str,
) -> None:
    process = await start_media_process(
        *args,
        stdout=asyncio.subprocess.DEVNULL,
        missing_error=missing_error,
    )
    _, stderr = await communicate_media_process(
        process,
        timeout=STICKER_TRANSCODE_TIMEOUT,
        timeout_error=f"{failure_prefix}：处理超时",
    )
    if process.returncode != 0:
        raise ValueError(f"{failure_prefix}: {decode_process_error(stderr)}")


def _render_png(media: MediaFile, output_path: Path) -> None:
    media.rewind()
    with Image.open(media.file) as image:
        image.load()
        image.convert("RGBA").save(output_path, format="PNG", optimize=True)

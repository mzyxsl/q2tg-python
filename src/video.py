"""把 Telegram 视频规范化为 OneBot 客户端普遍可播放的 MP4。"""

import asyncio
import json
import time
from pathlib import Path

from src.log import baselog
from src.media import (
    FFMPEG_BASE_ARGS,
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
from src.runtime_events import emit_runtime_event

PROBE_TIMEOUT = 30
TRANSCODE_TIMEOUT = 300


async def normalize_video_for_onebot(media: MediaFile, *, size_limit: int) -> None:
    """按需将视频原地替换为 H.264 + AAC MP4。

    Telegram 接受 HEVC MP4，但部分 OneBot 客户端播放时会黑屏；如果客户端显示
    “视频已过期”，应升级 SnowLuma。
    已兼容的 H.264/AAC 文件保持原样；其它编码通过 ffmpeg 转码。转码结果先写入
    独立临时路径，成功且未超限后再覆盖原 MediaFile，异常时原文件保持可清理。
    """
    video_codec, audio_codecs = await _probe_codecs(media)
    if video_codec == "h264" and all(codec == "aac" for codec in audio_codecs):
        emit_runtime_event("capability.succeeded", "telegram.video.compatible")
        return

    started_at = time.monotonic()
    async with transcode_target(".mp4") as output_path:
        async with media_input_path(
            media,
            suffix=Path(media.filename).suffix or ".video",
        ) as (input_path, process_kwargs):
            process = await start_media_process(
                *FFMPEG_BASE_ARGS,
                "-i",
                input_path,
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-movflags",
                "+faststart",
                "-fs",
                str(size_limit + 1),
                str(output_path),
                **process_kwargs,
                missing_error="视频转发需要安装 ffmpeg 和 ffprobe",
            )
            _, stderr = await communicate_media_process(
                process,
                timeout=TRANSCODE_TIMEOUT,
                timeout_error="Telegram 视频处理超时",
            )
        if process.returncode != 0:
            raise ValueError(f"Telegram 视频转码失败: {decode_process_error(stderr)}")
        if output_path.stat().st_size > size_limit:
            raise ValueError(
                f"Telegram 视频转码后超过 {TELEGRAM_DOWNLOAD_LIMIT_TEXT} 上限"
            )

        await run_media_thread(
            finalize_media,
            media,
            output_path,
            stem_fallback="video",
            suffix=".mp4",
            media_type="video/mp4",
        )
        emit_runtime_event("capability.succeeded", "telegram.video.transcoded")
        baselog.info("视频转码完成，耗时 %.2f 秒", time.monotonic() - started_at)


async def _probe_codecs(media: MediaFile) -> tuple[str | None, tuple[str, ...]]:
    async with media_input_path(
        media,
        suffix=Path(media.filename).suffix or ".video",
    ) as (input_path, process_kwargs):
        process = await start_media_process(
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type,codec_name",
            "-of",
            "json",
            input_path,
            stdout=asyncio.subprocess.PIPE,
            **process_kwargs,
            missing_error="视频转发需要安装 ffmpeg 和 ffprobe",
        )
        stdout, stderr = await communicate_media_process(
            process,
            timeout=PROBE_TIMEOUT,
            timeout_error="Telegram 视频处理超时",
        )
    if process.returncode != 0:
        raise ValueError(f"Telegram 视频格式检测失败: {decode_process_error(stderr)}")
    try:
        streams = json.loads(stdout).get("streams", [])
    except (json.JSONDecodeError, AttributeError):
        raise ValueError("Telegram 视频格式检测失败") from None
    video_codec: str | None = next(
        (
            stream.get("codec_name")
            for stream in streams
            if stream.get("codec_type") == "video"
            and isinstance(stream.get("codec_name"), str)
        ),
        None,
    )
    audio_codecs = tuple(
        stream.get("codec_name")
        for stream in streams
        if stream.get("codec_type") == "audio" and isinstance(stream.get("codec_name"), str)
    )
    if video_codec is None:
        raise ValueError("Telegram 视频中没有可用的视频流")
    return video_codec, audio_codecs

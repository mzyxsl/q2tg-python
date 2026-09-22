from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from telegram import LinkPreviewOptions
from telegram.ext import ExtBot

from src.forwarding import (
    forward_onebot_to_telegram,
    forward_telegram_to_onebot,
    onebot_message_media,
    onebot_message_text,
)
from src.media import MediaFile, media_cache, media_item_budget
from src.messages import (
    MediaTooLargeError,
    OneBotMessage,
    OneBotSendError,
    TelegramMedia,
    TelegramMessage,
)
from src.qbot import QGateway
from src.sql import Sql


@pytest.mark.asyncio
class TestVideoForwarding:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        self.directory = TemporaryDirectory()
        self.database = Sql(Path(self.directory.name) / "video.sqlite3")
        await self.database.load()
        await self.database.bind_group(123, -456)
        try:
            yield
        finally:
            media_cache.close()
            await self.database.close()
            self.directory.cleanup()

    async def test_telegram_video_sends_text_and_video_separately(self) -> None:
        initial_items = media_item_budget.used
        video = await MediaFile.create(filename="clip.mp4", media_type="video/mp4")
        video.write(b"video")
        gateway = SimpleNamespace(send_group_message=AsyncMock(side_effect=[99, 100]))
        message = TelegramMessage(
            message_ids=(200,),
            group_id=-456,
            user_id=2,
            sender_name="Telegram User",
            text="caption",
            media=(TelegramMedia(kind="video", content=video),),
        )

        with patch("src.forwarding.sql", self.database):
            await forward_telegram_to_onebot(message, cast(QGateway, gateway))

        assert gateway.send_group_message.await_count == 2
        first_segments = gateway.send_group_message.await_args_list[0].kwargs["message"]
        second_segments = gateway.send_group_message.await_args_list[1].kwargs["message"]
        assert first_segments == [
            {"type": "text", "data": {"text": "Telegram User:\ncaption"}}
        ]
        assert second_segments[0]["type"] == "video"
        assert "/media/" in second_segments[0]["data"]["file"]
        mapping = await self.database.get_q_message(-456, 200)
        assert mapping is not None
        assert mapping.q_message_ids == (99, 100)
        first_mapping = await self.database.get_tg_message(123, 99)
        assert first_mapping is not None

        media_cache.close()
        assert media_item_budget.used == initial_items

    async def test_onebot_media_uses_file_as_filename_and_url_as_download_source(self) -> None:
        media, unavailable = onebot_message_media(
            [
                {
                    "type": "video",
                    "data": {
                        "file": "clip.mp4",
                        "url": "https://example.test/download/video?id=123",
                    },
                },
                {
                    "type": "image",
                    "data": {
                        "file": "image.jpg",
                        "url": "https://example.test/download/image?id=456",
                    },
                },
                {
                    "type": "file",
                    "data": {
                        "file": "archive.zip",
                        "url": "https://example.test/download/file?id=789",
                    },
                },
            ]
        )

        assert unavailable == []
        assert media[0][1] == "https://example.test/download/video?id=123"
        assert media[0][2] == "clip.mp4"
        assert media[1][1] == "https://example.test/download/image?id=456"
        assert media[1][2] == "image.jpg"
        assert media[2][0] == "file"
        assert media[2][1] == "https://example.test/download/file?id=789"
        assert media[2][2] == "archive.zip"

    async def test_onebot_markdown_image_is_forwarded_as_media(self) -> None:
        image_url = "https://qqbot.ugcimg.cn/path/result"
        content = f"**查询结果**\n\n![img #1280px #1280px]\\([{image_url}]({image_url}))"
        media, unavailable = onebot_message_media(
            [{"type": "markdown", "data": {"content": content}}]
        )

        assert media == [("image", image_url, "result")]
        assert unavailable == []
        assert await onebot_message_text(
            [{"type": "markdown", "data": {"content": content}}],
            123,
        ) == "查询结果"

        cq_content = (
            "[CQ:markdown,content=[](%7B%22version%22%3A2%7D)\n" + content
        )
        cq_media, cq_unavailable = onebot_message_media(
            [{"type": "text", "data": {"text": cq_content}}]
        )
        assert cq_media == [("image", image_url, "result")]
        assert cq_unavailable == []
        assert await onebot_message_text(
            [{"type": "text", "data": {"text": cq_content}}],
            123,
        ) == "查询结果"

    async def test_onebot_file_uses_send_document(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"file",
                headers={"content-type": "application/octet-stream"},
                request=request,
            )

        bot = SimpleNamespace(
            send_document=AsyncMock(return_value=SimpleNamespace(message_id=205)),
        )
        message = OneBotMessage(
            message_id=105,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "file",
                    "data": {
                        "file": "large-video.mp4",
                        "url": "https://example.test/file",
                    },
                },
                {"type": "text", "data": {"text": "caption"}},
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_document.assert_awaited_once()
        assert bot.send_document.await_args.kwargs["caption"] == "OneBot User[1]:\ncaption"

    async def test_telegram_file_keeps_text_in_same_onebot_message(self) -> None:
        content = await MediaFile.create(
            filename="large-video.mp4",
            media_type="video/mp4",
        )
        content.write(b"file")
        gateway = SimpleNamespace(send_group_message=AsyncMock(return_value=106))
        message = TelegramMessage(
            message_ids=(206,),
            group_id=-456,
            user_id=2,
            sender_name="Telegram User",
            text="caption",
            forwarded_from="Original User",
            media=(TelegramMedia(kind="file", content=content),),
        )

        with patch("src.forwarding.sql", self.database):
            await forward_telegram_to_onebot(message, cast(QGateway, gateway))

        gateway.send_group_message.assert_awaited_once()
        segments = gateway.send_group_message.await_args.kwargs["message"]
        assert segments[0]["data"]["text"] == "Telegram User:\n转发自: Original User\ncaption"
        assert segments[1]["type"] == "file"
        assert "/media/" in segments[1]["data"]["file"]
        assert segments[1]["data"]["name"] == "large-video.mp4"

    async def test_telegram_video_retry_does_not_repeat_sent_text(self) -> None:
        video = await MediaFile.create(filename="clip.mp4", media_type="video/mp4")
        video.write(b"video")
        gateway = SimpleNamespace(
            send_group_message=AsyncMock(
                side_effect=[101, RuntimeError("video failed"), 102],
            )
        )
        message = TelegramMessage(
            message_ids=(207,),
            group_id=-456,
            user_id=2,
            sender_name="Telegram User",
            text="caption",
            media=(TelegramMedia(kind="video", content=video),),
        )

        with patch("src.forwarding.sql", self.database):
            with pytest.raises(OneBotSendError):
                await forward_telegram_to_onebot(message, cast(QGateway, gateway))
            await forward_telegram_to_onebot(message, cast(QGateway, gateway))

        assert gateway.send_group_message.await_count == 3
        assert gateway.send_group_message.await_args_list[0].kwargs["message"] == [
            {"type": "text", "data": {"text": "Telegram User:\ncaption"}}
        ]
        assert (
            gateway.send_group_message.await_args_list[1].kwargs["message"]
            == gateway.send_group_message.await_args_list[2].kwargs["message"]
        )

        mapping = await self.database.get_tg_message(123, 101)
        assert mapping is not None
        assert mapping.q_message_ids == (101, 102)

    async def test_onebot_mp4_uses_send_video_and_saves_mapping(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso2",
                headers={"content-type": "video/mp4"},
                request=request,
            )

        bot = SimpleNamespace(
            send_video=AsyncMock(return_value=SimpleNamespace(message_id=201)),
            send_document=AsyncMock(),
        )
        message = OneBotMessage(
            message_id=101,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {"type": "video", "data": {"file": "clip.mp4", "url": "https://example.test/video"}},
                {"type": "text", "data": {"text": "caption"}},
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_video.assert_awaited_once()
        assert bot.send_video.await_args.kwargs["supports_streaming"]
        assert bot.send_video.await_args.kwargs["show_caption_above_media"]
        assert bot.send_video.await_args.kwargs["caption"] == "OneBot User[1]:\ncaption"
        bot.send_document.assert_not_awaited()
        mapping = await self.database.get_tg_message(123, 101)
        assert mapping is not None
        assert mapping.tg_message_ids == (201,)

    async def test_onebot_video_with_existing_mapping_is_not_sent_again(self) -> None:
        await self.database.set_message_mapping(
            q_group_id=123,
            q_message_ids=(-101,),
            tg_chat_id=-456,
            tg_message_ids=(201,),
        )
        bot = SimpleNamespace(send_video=AsyncMock(), send_document=AsyncMock())
        message = OneBotMessage(
            message_id=-101,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "video",
                    "data": {
                        "file": "clip.mp4",
                        "url": "https://example.test/video",
                    },
                }
            ],
        )

        async with httpx.AsyncClient() as client:
            with (
                patch("src.forwarding.sql", self.database),
                patch("src.forwarding.download_media", new_callable=AsyncMock) as download,
            ):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        download.assert_not_awaited()
        bot.send_video.assert_not_awaited()
        bot.send_document.assert_not_awaited()

    async def test_fake_mp4_is_sent_as_document(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"not an mp4",
                headers={"content-type": "video/mp4"},
                request=request,
            )

        bot = SimpleNamespace(
            send_video=AsyncMock(),
            send_document=AsyncMock(return_value=SimpleNamespace(message_id=201)),
        )
        message = OneBotMessage(
            message_id=101,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "video",
                    "data": {
                        "file": "clip.mp4",
                        "url": "https://example.test/video",
                    },
                }
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_video.assert_not_awaited()
        bot.send_document.assert_awaited_once()

    async def test_disabled_id_show_hides_only_fallback_user_id(self) -> None:
        await self.database.set_id_show_enabled(-456, False)
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=209)),
        )
        message = OneBotMessage(
            message_id=111,
            group_id=123,
            user_id=234,
            sender_name="OneBot 用户[234]",
            sender_name_is_fallback=True,
            message=[{"type": "text", "data": {"text": "message"}}],
        )

        async with httpx.AsyncClient() as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_message.assert_awaited_once_with(
            chat_id=-456,
            text="OneBot 用户:\nmessage",
            reply_parameters=None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

    async def test_disabled_id_show_keeps_real_sender_name(self) -> None:
        await self.database.set_id_show_enabled(-456, False)
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=210)),
        )
        message = OneBotMessage(
            message_id=112,
            group_id=123,
            user_id=234,
            sender_name="Named User",
            message=[{"type": "text", "data": {"text": "message"}}],
        )

        async with httpx.AsyncClient() as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_message.assert_awaited_once_with(
            chat_id=-456,
            text="Named User:\nmessage",
            reply_parameters=None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

    async def test_enabled_id_show_appends_user_id_to_real_sender_name(self) -> None:
        await self.database.set_id_show_enabled(-456, True)
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=211)),
        )
        message = OneBotMessage(
            message_id=113,
            group_id=123,
            user_id=234,
            sender_name="Example User",
            message=[{"type": "text", "data": {"text": "message"}}],
        )

        async with httpx.AsyncClient() as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_message.assert_awaited_once_with(
            chat_id=-456,
            text="Example User[234]:\nmessage",
            reply_parameters=None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

    async def test_enabled_id_show_does_not_duplicate_fallback_user_id(self) -> None:
        await self.database.set_id_show_enabled(-456, True)
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=212)),
        )
        message = OneBotMessage(
            message_id=114,
            group_id=123,
            user_id=234,
            sender_name="OneBot 用户[234]",
            sender_name_is_fallback=True,
            message=[{"type": "text", "data": {"text": "message"}}],
        )

        async with httpx.AsyncClient() as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_message.assert_awaited_once_with(
            chat_id=-456,
            text="OneBot 用户[234]:\nmessage",
            reply_parameters=None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )

    async def test_onebot_non_mp4_video_is_sent_as_document(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"video",
                headers={"content-type": "video/webm"},
                request=request,
            )

        bot = SimpleNamespace(
            send_video=AsyncMock(),
            send_document=AsyncMock(return_value=SimpleNamespace(message_id=202)),
        )
        message = OneBotMessage(
            message_id=102,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {"type": "video", "data": {"file": "clip.webm", "url": "https://example.test/video"}},
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_document.assert_awaited_once()
        bot.send_video.assert_not_awaited()

    async def test_onebot_image_over_photo_limit_is_sent_as_document(self) -> None:
        content = await MediaFile.create(filename="large.jpg", media_type="image/jpeg")
        content.size = 10_000_001
        bot = SimpleNamespace(
            send_photo=AsyncMock(),
            send_document=AsyncMock(return_value=SimpleNamespace(message_id=204)),
        )
        message = OneBotMessage(
            message_id=106,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {"file": "large.jpg", "url": "https://example.test/image"},
                },
            ],
        )

        async with httpx.AsyncClient() as client:
            with (
                patch("src.forwarding.sql", self.database),
                patch("src.forwarding.download_media", new_callable=AsyncMock, return_value=content),
            ):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_document.assert_awaited_once()
        bot.send_photo.assert_not_awaited()

    async def test_onebot_single_photo_shows_caption_above_media(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"image",
                headers={"content-type": "image/jpeg"},
                request=request,
            )

        bot = SimpleNamespace(
            send_photo=AsyncMock(return_value=SimpleNamespace(message_id=205)),
        )
        message = OneBotMessage(
            message_id=108,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {"file": "image.jpg", "url": "https://example.test/image"},
                },
                {"type": "text", "data": {"text": "caption"}},
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_photo.assert_awaited_once()
        assert bot.send_photo.await_args.kwargs["show_caption_above_media"]
        assert bot.send_photo.await_args.kwargs["caption"] == "OneBot User[1]:\ncaption"

    async def test_onebot_gif_image_is_sent_as_animation(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"GIF89a" + b"animation",
                headers={"content-type": "image/gif"},
                request=request,
            )

        bot = SimpleNamespace(
            send_animation=AsyncMock(return_value=SimpleNamespace(message_id=206)),
            send_photo=AsyncMock(),
        )
        message = OneBotMessage(
            message_id=109,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {
                        "file": "animation.image",
                        "url": "https://example.test/animation",
                    },
                },
                {"type": "text", "data": {"text": "caption"}},
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_animation.assert_awaited_once()
        bot.send_photo.assert_not_awaited()
        kwargs = bot.send_animation.await_args.kwargs
        assert kwargs["animation"].filename == "animation.gif"
        assert kwargs["caption"] == "OneBot User[1]:\ncaption"
        assert kwargs["show_caption_above_media"]

    async def test_gif_in_image_group_disables_telegram_album(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("gif"):
                return httpx.Response(
                    200,
                    content=b"GIF89a" + b"animation",
                    headers={"content-type": "application/octet-stream"},
                    request=request,
                )
            return httpx.Response(
                200,
                content=b"image",
                headers={"content-type": "image/jpeg"},
                request=request,
            )

        bot = SimpleNamespace(
            send_photo=AsyncMock(return_value=SimpleNamespace(message_id=207)),
            send_animation=AsyncMock(return_value=SimpleNamespace(message_id=208)),
            send_media_group=AsyncMock(),
        )
        message = OneBotMessage(
            message_id=110,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {"file": "photo.jpg", "url": "https://example.test/photo"},
                },
                {
                    "type": "image",
                    "data": {"file": "emoji", "url": "https://example.test/gif"},
                },
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_media_group.assert_not_awaited()
        bot.send_photo.assert_awaited_once()
        bot.send_animation.assert_awaited_once()
        assert message.tg_message_ids == [207, 208]

    async def test_onebot_multiple_images_are_sent_as_retryable_telegram_album(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"image",
                headers={"content-type": "image/jpeg"},
                request=request,
            )

        bot = SimpleNamespace(
            send_media_group=AsyncMock(
                side_effect=[
                    RuntimeError("album failed"),
                    [
                        SimpleNamespace(message_id=301),
                        SimpleNamespace(message_id=302),
                        SimpleNamespace(message_id=303),
                    ],
                ]
            ),
        )
        message = OneBotMessage(
            message_id=104,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {"file": "first.jpg", "url": "https://example.test/first"},
                },
                {
                    "type": "image",
                    "data": {"file": "second.jpg", "url": "https://example.test/second"},
                },
                {
                    "type": "image",
                    "data": {"file": "third.jpg", "url": "https://example.test/third"},
                },
                {"type": "text", "data": {"text": "测试"}},
            ],
        )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch("src.forwarding.sql", self.database):
                with pytest.raises(RuntimeError, match="album failed"):
                    await forward_onebot_to_telegram(
                        message,
                        cast(ExtBot[None], bot),
                        client,
                    )
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        assert bot.send_media_group.await_count == 2
        album = bot.send_media_group.await_args.kwargs["media"]
        assert len(album) == 3
        assert album[0].caption == "OneBot User[1]:\n测试"
        assert album[1].caption is None
        assert album[2].caption is None
        # Telegram 要求媒体组内所有项的 show_caption_above_media 一致，否则整组被拒。
        assert all(item.show_caption_above_media for item in album)
        attach_uris = [item.media.attach_uri for item in album]
        assert all(uri and uri.startswith("attach://") for uri in attach_uris)
        assert len(set(attach_uris)) == 3
        assert message.tg_message_ids == [301, 302, 303]
        mapping = await self.database.get_tg_message(123, 104)
        assert mapping is not None
        assert mapping.tg_message_ids == (301, 302, 303)

    async def test_one_large_image_makes_whole_album_a_document_group(self) -> None:
        contents = []
        for index, size in enumerate((5, 10_000_001, 5)):
            content = await MediaFile.create(
                filename=f"image-{index}.jpg",
                media_type="image/jpeg",
            )
            content.size = size
            contents.append(content)
        bot = SimpleNamespace(
            send_media_group=AsyncMock(
                return_value=[
                    SimpleNamespace(message_id=401),
                    SimpleNamespace(message_id=402),
                    SimpleNamespace(message_id=403),
                ]
            ),
        )
        message = OneBotMessage(
            message_id=107,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {
                        "file": f"image-{index}.jpg",
                        "url": f"https://example.test/image-{index}",
                    },
                }
                for index in range(3)
            ]
            + [{"type": "text", "data": {"text": "测试"}}],
        )

        async with httpx.AsyncClient() as client:
            with (
                patch("src.forwarding.sql", self.database),
                patch(
                    "src.forwarding.download_media",
                    new_callable=AsyncMock,
                    side_effect=contents,
                ),
            ):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        album = bot.send_media_group.await_args.kwargs["media"]
        assert [item.type for item in album] == ["document"] * 3
        assert album[0].caption is None
        assert album[1].caption is None
        assert album[2].caption == "OneBot User[1]:\n测试"
        attach_uris = [item.media.attach_uri for item in album]
        assert all(uri and uri.startswith("attach://") for uri in attach_uris)
        assert len(set(attach_uris)) == 3
        mapping = await self.database.get_tg_message(123, 107)
        assert mapping is not None
        assert mapping.tg_message_ids == (401, 402, 403)

    async def test_onebot_image_album_over_request_body_limit_is_rejected(self) -> None:
        # 云端 Bot API 实测阈值为 50 MiB 请求体；10 张合计 53.30 MiB 会返回
        # 413 Request Entity Too Large，因此发送前必须按合计字节数拦截。
        contents = []
        for index in range(3):
            content = await MediaFile.create(
                filename=f"large-{index}.jpg",
                media_type="image/jpeg",
            )
            content.size = 20 * 1024 * 1024
            contents.append(content)
        bot = SimpleNamespace(send_media_group=AsyncMock())
        message = OneBotMessage(
            message_id=109,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {
                        "file": f"large-{index}.jpg",
                        "url": f"https://example.test/large-{index}",
                    },
                }
                for index in range(3)
            ],
        )

        async with httpx.AsyncClient() as client:
            with (
                patch("src.forwarding.sql", self.database),
                patch(
                    "src.forwarding.download_media",
                    new_callable=AsyncMock,
                    side_effect=contents,
                ),
                pytest.raises(MediaTooLargeError, match="图片组超过 49 MiB"),
            ):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_media_group.assert_not_awaited()
        # 第三张就已超限，最后一张不应被下载。
        assert contents[2].file.closed
        assert all(content.file.closed for content in contents)

    async def test_onebot_image_album_just_under_request_body_limit_is_sent(self) -> None:
        # 49.89 MiB 实测通过，因此贴近上限但未超出的图片组必须照常发送。
        contents = []
        for index in range(3):
            content = await MediaFile.create(
                filename=f"edge-{index}.jpg",
                media_type="image/jpeg",
            )
            content.size = 16 * 1024 * 1024
            contents.append(content)
        bot = SimpleNamespace(
            send_media_group=AsyncMock(
                return_value=[
                    SimpleNamespace(message_id=501),
                    SimpleNamespace(message_id=502),
                    SimpleNamespace(message_id=503),
                ]
            ),
        )
        message = OneBotMessage(
            message_id=110,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[
                {
                    "type": "image",
                    "data": {
                        "file": f"edge-{index}.jpg",
                        "url": f"https://example.test/edge-{index}",
                    },
                }
                for index in range(3)
            ],
        )

        async with httpx.AsyncClient() as client:
            with (
                patch("src.forwarding.sql", self.database),
                patch(
                    "src.forwarding.download_media",
                    new_callable=AsyncMock,
                    side_effect=contents,
                ),
            ):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_media_group.assert_awaited_once()
        # 每张 16 MiB 超过 sendPhoto 的 10 MB，整组应作为文件组发送。
        album = bot.send_media_group.await_args.kwargs["media"]
        assert [item.type for item in album] == ["document"] * 3

    async def test_onebot_video_without_url_sends_mapped_placeholder(self) -> None:
        bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(message_id=203)),
        )
        message = OneBotMessage(
            message_id=103,
            group_id=123,
            user_id=1,
            sender_name="OneBot User",
            message=[{"type": "video", "data": {"file": "clip.mp4"}}],
        )

        async with httpx.AsyncClient() as client:
            with patch("src.forwarding.sql", self.database):
                await forward_onebot_to_telegram(
                    message,
                    cast(ExtBot[None], bot),
                    client,
                )

        bot.send_message.assert_awaited_once_with(
            chat_id=-456,
            text="OneBot User[1]:\n[视频无法转发：缺少可用的 HTTP(S) 下载地址]",
            reply_parameters=None,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        mapping = await self.database.get_tg_message(123, 103)
        assert mapping is not None

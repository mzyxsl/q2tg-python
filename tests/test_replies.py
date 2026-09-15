import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from telegram.ext import ExtBot

from src.forwarding import (
    begin_telegram_replacement,
    finish_telegram_replacement,
    forward_onebot_to_telegram,
    forward_telegram_to_onebot,
)
from src.messages import OneBotMessage, TelegramMessage
from src.qbot import QGateway
from src.sql import Sql


@pytest.mark.asyncio
class TestReplyForwarding:
    @pytest_asyncio.fixture(autouse=True)
    async def setup_database(self):
        self.directory = TemporaryDirectory()
        self.cache = Sql(Path(self.directory.name) / "cache.sqlite3")
        await self.cache.load()
        await self.cache.bind_group(123, -456)
        await self.cache.set_message_mapping(
            q_group_id=123,
            q_message_ids=(99, 100),
            tg_chat_id=-456,
            tg_message_ids=(200, 201),
        )
        try:
            yield
        finally:
            await self.cache.close()
            self.directory.cleanup()

    async def test_onebot_reply_uses_first_telegram_message(self) -> None:
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=202)))
        async with httpx.AsyncClient() as client:
            with patch("src.forwarding.sql", self.cache):
                await forward_onebot_to_telegram(
                    OneBotMessage(
                        message_id=101,
                        group_id=123,
                        user_id=1,
                        sender_name="OneBot User",
                        message=[{"type": "text", "data": {"text": "reply"}}],
                        reply_message_id=100,
                    ),
                    cast(ExtBot[None], bot),
                    client,
                )

        reply_parameters = bot.send_message.await_args.kwargs["reply_parameters"]
        assert reply_parameters.message_id == 200

    async def test_telegram_reply_adds_onebot_reply_segment(self) -> None:
        gateway = SimpleNamespace(send_group_message=AsyncMock(return_value=102))
        with patch("src.forwarding.sql", self.cache):
            await forward_telegram_to_onebot(
                TelegramMessage(
                    message_ids=(202,),
                    group_id=-456,
                    user_id=2,
                    sender_name="Telegram User",
                    text="reply",
                    reply_message_id=201,
                ),
                cast(QGateway, gateway),
            )

        segments = gateway.send_group_message.await_args.kwargs["message"]
        assert segments[0] == {"type": "reply", "data": {"id": "100"}}

    async def test_edited_telegram_message_replaces_old_onebot_mapping(self) -> None:
        gateway = SimpleNamespace(
            delete_message=AsyncMock(),
            send_group_message=AsyncMock(side_effect=[102, 103]),
        )
        with patch("src.forwarding.sql", self.cache):
            await forward_telegram_to_onebot(
                TelegramMessage(
                    message_ids=(200,),
                    group_id=-456,
                    user_id=2,
                    sender_name="Telegram User",
                    text="edited",
                    replace_existing=True,
                ),
                cast(QGateway, gateway),
            )
            await forward_telegram_to_onebot(
                TelegramMessage(
                    message_ids=(202,),
                    group_id=-456,
                    user_id=2,
                    sender_name="Telegram User",
                    text="reply",
                    reply_message_id=200,
                ),
                cast(QGateway, gateway),
            )

        assert gateway.delete_message.await_args_list[0].args == (99,)
        assert gateway.delete_message.await_args_list[1].args == (100,)
        edited_segments = gateway.send_group_message.await_args_list[0].kwargs["message"]
        assert edited_segments == [{"type": "text", "data": {"text": "Telegram User:\nedited"}}]
        reply_segments = gateway.send_group_message.await_args_list[1].kwargs["message"]
        assert reply_segments[0] == {"type": "reply", "data": {"id": "102"}}

    async def test_reply_waits_for_pending_edit_before_reading_mapping(self) -> None:
        gateway = SimpleNamespace(send_group_message=AsyncMock(return_value=103))
        begin_telegram_replacement(-456, (200,))
        with patch("src.forwarding.sql", self.cache):
            task = asyncio.create_task(
                forward_telegram_to_onebot(
                    TelegramMessage(
                        message_ids=(202,),
                        group_id=-456,
                        user_id=2,
                        sender_name="Telegram User",
                        text="reply",
                        reply_message_id=200,
                    ),
                    cast(QGateway, gateway),
                )
            )
            await asyncio.sleep(0)
            gateway.send_group_message.assert_not_awaited()
            await self.cache.set_message_mapping(
                q_group_id=123,
                q_message_ids=(102,),
                tg_chat_id=-456,
                tg_message_ids=(200,),
            )
            finish_telegram_replacement(-456, (200,))
            await task

        segments = gateway.send_group_message.await_args.kwargs["message"]
        assert segments[0] == {"type": "reply", "data": {"id": "102"}}

    async def test_missing_mapping_falls_back_to_normal_message(self) -> None:
        gateway = SimpleNamespace(send_group_message=AsyncMock(return_value=103))
        with patch("src.forwarding.sql", self.cache):
            await forward_telegram_to_onebot(
                TelegramMessage(
                    message_ids=(203,),
                    group_id=-456,
                    user_id=2,
                    sender_name="Telegram User",
                    text="reply",
                    reply_message_id=999,
                ),
                cast(QGateway, gateway),
            )

        segments = gateway.send_group_message.await_args.kwargs["message"]
        assert segments[0]["type"] != "reply"

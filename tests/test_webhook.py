from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.alert_engine import Alert
from app.config import WebhookConfig
from app.webhook import WebhookWorker


def _alert() -> Alert:
    return Alert(
        symbol="BTCUSDT",
        price=123456.78,
        direction="up",
        tier=1,
        movement_pct=3.2,
        message="test alert",
    )


class WebhookDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def worker(self, *, max_retries: int = 2) -> WebhookWorker:
        config = WebhookConfig(
            url="https://example.com/hook",
            timeout_seconds=10,
            max_retries=max_retries,
        )
        return WebhookWorker(config, asyncio.Queue())

    async def test_success_is_delivered_once_as_ticker_price_json(self) -> None:
        client = AsyncMock()
        client.post.return_value = httpx.Response(204)

        await self.worker()._deliver(client, _alert())

        client.post.assert_awaited_once_with(
            "https://example.com/hook",
            json={"ticker": "BTCUSDT", "price": "123456.78"},
        )

    async def test_non_retryable_response_stops_immediately(self) -> None:
        client = AsyncMock()
        client.post.return_value = httpx.Response(400)

        await self.worker()._deliver(client, _alert())

        self.assertEqual(client.post.await_count, 1)

    async def test_retryable_response_uses_bounded_backoff(self) -> None:
        client = AsyncMock()
        client.post.side_effect = [
            httpx.Response(500),
            httpx.Response(429),
            httpx.Response(200),
        ]

        with patch("app.webhook.asyncio.sleep", new=AsyncMock()) as sleep:
            await self.worker(max_retries=2)._deliver(client, _alert())

        self.assertEqual(client.post.await_count, 3)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [1, 2])

    async def test_network_error_exhausts_configured_attempts(self) -> None:
        client = AsyncMock()
        request = httpx.Request("POST", "https://example.com/hook")
        client.post.side_effect = httpx.ConnectError("offline", request=request)

        with patch("app.webhook.asyncio.sleep", new=AsyncMock()) as sleep:
            await self.worker(max_retries=1)._deliver(client, _alert())

        self.assertEqual(client.post.await_count, 2)
        sleep.assert_awaited_once_with(1)


if __name__ == "__main__":
    unittest.main()

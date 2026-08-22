from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import httpx

from app.alert_engine import Alert
from app.config import WebhookConfig
from app.webhook import WebhookWorker, _retry_delay


def _alert() -> Alert:
    return Alert(
        symbol="BTCUSDT",
        price=123456.78,
        direction="up",
        movement_pct=3.2,
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

        with self.assertLogs("app.webhook", level="ERROR") as logs:
            await self.worker()._deliver(client, _alert())

        client.post.assert_awaited_once_with(
            "https://example.com/hook",
            json={"ticker": "BTCUSDT", "price": "123456.78"},
        )
        self.assertEqual(len(logs.records), 1)
        self.assertIn(
            "CALL triggered: ticker=BTCUSDT price=123456.78",
            logs.records[0].getMessage(),
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

    async def test_request_timeout_response_is_retried(self) -> None:
        client = AsyncMock()
        client.post.side_effect = [httpx.Response(408), httpx.Response(200)]

        with patch("app.webhook.asyncio.sleep", new=AsyncMock()) as sleep:
            await self.worker(max_retries=1)._deliver(client, _alert())

        self.assertEqual(client.post.await_count, 2)
        sleep.assert_awaited_once_with(1)

    async def test_retry_after_header_is_respected_and_capped(self) -> None:
        client = AsyncMock()
        client.post.side_effect = [
            httpx.Response(429, headers={"Retry-After": "90"}),
            httpx.Response(200),
        ]

        with patch("app.webhook.asyncio.sleep", new=AsyncMock()) as sleep:
            await self.worker(max_retries=1)._deliver(client, _alert())

        sleep.assert_awaited_once_with(30)

    def test_retry_delay_is_bounded(self) -> None:
        self.assertEqual(
            [_retry_delay(attempt) for attempt in range(1, 8)],
            [1, 2, 4, 8, 16, 30, 30],
        )
        self.assertEqual(_retry_delay(1_000_000), 30)
        self.assertEqual(_retry_delay(1, "2.2"), 3)
        self.assertEqual(_retry_delay(1, "not-a-number"), 1)

    def test_retry_delay_supports_standard_http_date(self) -> None:
        now = datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc)

        self.assertEqual(
            _retry_delay(
                1,
                "Sat, 22 Aug 2026 00:00:12 GMT",
                now=now,
            ),
            12,
        )
        self.assertEqual(
            _retry_delay(
                1,
                "Sat, 22 Aug 2026 00:01:00 GMT",
                now=now,
            ),
            30,
        )

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

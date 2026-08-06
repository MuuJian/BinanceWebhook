"""Non-blocking plain-text Webhook delivery with bounded retries."""

from __future__ import annotations

import asyncio
import logging

import httpx

from app.alert_engine import Alert
from app.config import WebhookConfig

logger = logging.getLogger(__name__)


class WebhookWorker:
    def __init__(
        self, config: WebhookConfig, queue: asyncio.Queue[Alert]
    ) -> None:
        self.config = config
        self.queue = queue

    async def run(self) -> None:
        timeout = httpx.Timeout(self.config.timeout_seconds)
        # trust_env=False prevents HTTP(S)_PROXY from silently routing the
        # private Webhook request. The full URL is never logged.
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            while True:
                alert = await self.queue.get()
                try:
                    await self._deliver(client, alert)
                finally:
                    self.queue.task_done()

    async def _deliver(self, client: httpx.AsyncClient, alert: Alert) -> None:
        attempts = self.config.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                response = await client.post(
                    self.config.url,
                    content=alert.message.encode("utf-8"),
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                )
                if 200 <= response.status_code < 300:
                    logger.info(
                        "Webhook delivered: symbol=%s status=%s attempt=%s",
                        alert.symbol,
                        response.status_code,
                        attempt,
                    )
                    return
                retryable = (
                    response.status_code == 429 or response.status_code >= 500
                )
                if not retryable:
                    logger.error(
                        "Webhook rejected: symbol=%s status=%s; not retrying",
                        alert.symbol,
                        response.status_code,
                    )
                    return
                detail = f"HTTP {response.status_code}"
            except httpx.RequestError as exc:
                detail = type(exc).__name__

            if attempt >= attempts:
                logger.error(
                    "Webhook failed after %s attempts: symbol=%s reason=%s",
                    attempts,
                    alert.symbol,
                    detail,
                )
                return
            delay = 2 ** (attempt - 1)
            logger.warning(
                "Webhook attempt failed: symbol=%s reason=%s; retrying in %ss",
                alert.symbol,
                detail,
                delay,
            )
            await asyncio.sleep(delay)

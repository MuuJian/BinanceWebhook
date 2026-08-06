"""Pure-text fwalert Webhook delivery with bounded exponential retries."""

from __future__ import annotations

import asyncio
import logging

import httpx

from app.evaluator import Alert

logger = logging.getLogger(__name__)


class WebhookSender:
    def __init__(
        self,
        *,
        url: str,
        timeout_seconds: float,
        max_retries: int,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if max_retries < 0:
            raise ValueError("max_retries must not be negative")
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._client = client
        self._owns_client = client is None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=5, max_keepalive_connections=2)
            )

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, alert: Alert) -> bool:
        await self.start()
        assert self._client is not None

        for retry_number in range(self.max_retries + 1):
            try:
                response = await self._client.post(
                    self.url,
                    content=alert.message.encode("utf-8"),
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                    timeout=self.timeout_seconds,
                )
                if 200 <= response.status_code < 300:
                    logger.info(
                        "Webhook delivered: %s %s %.2f%% (status=%s)",
                        alert.symbol,
                        alert.direction,
                        abs(alert.change_pct),
                        response.status_code,
                    )
                    return True

                retryable = response.status_code == 429 or response.status_code >= 500
                error_summary = f"HTTP {response.status_code}"
            except (httpx.RequestError, OSError) as exc:
                retryable = True
                error_summary = type(exc).__name__

            if not retryable or retry_number >= self.max_retries:
                logger.error(
                    "Webhook delivery failed: %s %s %.2f%% (%s)",
                    alert.symbol,
                    alert.direction,
                    abs(alert.change_pct),
                    error_summary,
                )
                return False

            delay = 2**retry_number
            logger.warning(
                "Webhook failed for %s %s (%s); retry %s/%s in %ss",
                alert.symbol,
                alert.direction,
                error_summary,
                retry_number + 1,
                self.max_retries,
                delay,
            )
            await asyncio.sleep(delay)
        return False


class WebhookWorker:
    def __init__(
        self,
        *,
        queue: asyncio.Queue[Alert],
        sender: WebhookSender,
    ) -> None:
        self.queue = queue
        self.sender = sender

    async def run(self) -> None:
        while True:
            alert = await self.queue.get()
            try:
                await self.sender.send(alert)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Unexpected Webhook worker failure for %s %s: %s",
                    alert.symbol,
                    alert.direction,
                    type(exc).__name__,
                )
            finally:
                self.queue.task_done()

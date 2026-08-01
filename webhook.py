"""Non-blocking webhook delivery with bounded retries."""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

import httpx

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429}


def _safe_error_summary(exc: Exception) -> str:
    """Describe a delivery error without logging a potentially secret URL."""

    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return type(exc).__name__


class WebhookSender:
    def __init__(
        self,
        *,
        url: str,
        timeout_seconds: float,
        body_format: str = "json",
        max_attempts: int = 3,
        base_backoff_seconds: float = 1.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if body_format not in {"json", "text"}:
            raise ValueError("body_format must be either json or text")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero")
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.body_format = body_format
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self._client = client
        self._owns_client = client is None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
            )

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, payload: dict[str, Any]) -> bool:
        await self.start()
        assert self._client is not None
        symbol = payload.get("symbol", "unknown")

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await self._post(payload)
                response.raise_for_status()
                logger.info(
                    "Webhook delivered for %s (status=%s, attempt=%s)",
                    symbol,
                    response.status_code,
                    attempt,
                )
                return True
            except (httpx.HTTPError, OSError) as exc:
                retryable = self._is_retryable(exc)
                if not retryable or attempt == self.max_attempts:
                    logger.error(
                        "Webhook failed for %s after %s attempt(s): %s",
                        symbol,
                        attempt,
                        _safe_error_summary(exc),
                    )
                    return False

                delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Webhook attempt %s/%s failed for %s: %s; retrying in %.1fs",
                    attempt,
                    self.max_attempts,
                    symbol,
                    _safe_error_summary(exc),
                    delay,
                )
                await asyncio.sleep(delay)
        return False

    async def _post(self, payload: dict[str, Any]) -> httpx.Response:
        assert self._client is not None
        if self.body_format == "text":
            message = str(payload.get("message", "价格波动提醒"))
            return await self._client.post(
                self.url,
                content=message.encode("utf-8"),
                headers={"Content-Type": "text/plain; charset=utf-8"},
                timeout=self.timeout_seconds,
            )
        return await self._client.post(
            self.url,
            json=payload,
            timeout=self.timeout_seconds,
        )

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if not isinstance(exc, httpx.HTTPStatusError):
            return True
        status = exc.response.status_code
        return status >= 500 or status in RETRYABLE_STATUS_CODES


class WebhookDispatcher:
    """Run webhook I/O outside the WebSocket receive path."""

    def __init__(
        self,
        sender: WebhookSender,
        *,
        workers: int = 2,
        queue_size: int = 100,
    ) -> None:
        if workers <= 0 or queue_size <= 0:
            raise ValueError("workers and queue_size must be greater than zero")
        self.sender = sender
        self.workers = workers
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._tasks:
            return
        await self.sender.start()
        self._tasks = [
            asyncio.create_task(self._worker(), name=f"webhook-worker-{index + 1}")
            for index in range(self.workers)
        ]

    def enqueue(self, payload: dict[str, Any]) -> bool:
        try:
            self.queue.put_nowait(payload)
            logger.info("Webhook queued for %s", payload.get("symbol", "unknown"))
            return True
        except asyncio.QueueFull:
            logger.error(
                "Webhook queue is full; dropping alert for %s",
                payload.get("symbol", "unknown"),
            )
            return False

    async def stop(self) -> None:
        if not self._tasks:
            await self.sender.close()
            return

        queued_and_active = self.queue.qsize() + self.workers
        batches = max(1, math.ceil(queued_and_active / self.workers))
        retry_backoff = self.sender.base_backoff_seconds * (
            2 ** (self.sender.max_attempts - 1) - 1
        )
        per_batch = (
            self.sender.timeout_seconds * self.sender.max_attempts
            + retry_backoff
            + 5
        )
        shutdown_timeout = min(300.0, max(35.0, per_batch * batches))

        try:
            await asyncio.wait_for(self.queue.join(), timeout=shutdown_timeout)
        except asyncio.TimeoutError:
            logger.error("Timed out while draining the webhook queue during shutdown")
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
            await self.sender.close()

    async def _worker(self) -> None:
        while True:
            payload = await self.queue.get()
            try:
                await self.sender.send(payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Unexpected webhook worker error for %s: %s",
                    payload.get("symbol", "unknown"),
                    type(exc).__name__,
                )
            finally:
                self.queue.task_done()

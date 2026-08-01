"""Non-blocking webhook delivery with bounded retries."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


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
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.body_format = body_format
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self._client = client

    async def send(self, payload: dict[str, Any]) -> bool:
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient()
        symbol = payload.get("symbol", "unknown")
        try:
            for attempt in range(1, self.max_attempts + 1):
                try:
                    if self.body_format == "text":
                        message = str(payload.get("message", "价格波动提醒"))
                        response = await client.post(
                            self.url,
                            content=message.encode("utf-8"),
                            headers={
                                "Content-Type": "text/plain; charset=utf-8"
                            },
                            timeout=self.timeout_seconds,
                        )
                    else:
                        response = await client.post(
                            self.url,
                            json=payload,
                            timeout=self.timeout_seconds,
                        )
                    response.raise_for_status()
                    logger.info(
                        "Webhook delivered for %s (status=%s, attempt=%s)",
                        symbol,
                        response.status_code,
                        attempt,
                    )
                    return True
                except (httpx.HTTPError, OSError) as exc:
                    error_summary = _safe_error_summary(exc)
                    if attempt == self.max_attempts:
                        logger.error(
                            "Webhook failed permanently for %s after %s attempts: %s",
                            symbol,
                            attempt,
                            error_summary,
                        )
                        return False
                    delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Webhook attempt %s/%s failed for %s: %s; retrying in %.1fs",
                        attempt,
                        self.max_attempts,
                        symbol,
                        error_summary,
                        delay,
                    )
                    await asyncio.sleep(delay)
        finally:
            if owns_client:
                await client.aclose()
        return False


class WebhookDispatcher:
    """Runs webhook I/O outside the WebSocket receive path."""

    def __init__(
        self,
        sender: WebhookSender,
        *,
        workers: int = 2,
        queue_size: int = 100,
    ) -> None:
        self.sender = sender
        self.workers = workers
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(self._worker(), name=f"webhook-worker-{index}")
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
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=35)
        except asyncio.TimeoutError:
            logger.error("Timed out while draining the webhook queue during shutdown")
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

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

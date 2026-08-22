from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.config import AppConfig, WebhookConfig
from app.main import _background_failure, run_worker


def _config() -> AppConfig:
    return AppConfig(
        symbols=("BTCUSDT",),
        websocket_url="wss://example.com",
        websocket_proxy=None,
        window_seconds=300,
        threshold_pct=2,
        cooldown_seconds=30,
        webhook=WebhookConfig(
            url="https://example.com/hook",
            timeout_seconds=10,
            max_retries=0,
        ),
        log_level=20,
    )


class _WaitingWebhook:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    async def run(self) -> None:
        await asyncio.Event().wait()


class BackgroundFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_graceful_worker_completion_with_stop_is_not_failure(self) -> None:
        stop_task = asyncio.create_task(asyncio.sleep(0), name="stop-signal")
        worker_task = asyncio.create_task(asyncio.sleep(0), name="market")
        await asyncio.gather(stop_task, worker_task)

        failure = _background_failure({stop_task, worker_task}, stop_task)

        self.assertIsNone(failure)

    async def test_unexpected_worker_completion_is_failure(self) -> None:
        stop_task = asyncio.create_task(asyncio.sleep(60), name="stop-signal")
        worker_task = asyncio.create_task(asyncio.sleep(0), name="market")
        await worker_task

        failure = _background_failure({worker_task}, stop_task)

        self.assertIsInstance(failure, RuntimeError)
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)

    async def test_worker_exception_wins_even_when_stop_is_requested(self) -> None:
        async def fail() -> None:
            raise ValueError("boom")

        stop_task = asyncio.create_task(asyncio.sleep(0), name="stop-signal")
        worker_task = asyncio.create_task(fail(), name="market")
        await asyncio.gather(stop_task, worker_task, return_exceptions=True)

        failure = _background_failure({stop_task, worker_task}, stop_task)

        self.assertIsInstance(failure, ValueError)

    async def test_worker_exception_wins_over_other_unexpected_completion(self) -> None:
        async def fail() -> None:
            raise ValueError("boom")

        stop_task = asyncio.create_task(asyncio.sleep(60), name="stop-signal")
        failed_task = asyncio.create_task(fail(), name="market")
        completed_task = asyncio.create_task(asyncio.sleep(0), name="engine")
        await asyncio.gather(failed_task, completed_task, return_exceptions=True)

        failure = _background_failure(
            {failed_task, completed_task}, stop_task
        )

        self.assertIsInstance(failure, ValueError)
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)


class WorkerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_receiver_requested_stop_is_graceful(self) -> None:
        class StoppingReceiver:
            def __init__(self, **_kwargs) -> None:
                pass

            async def run(self, stop_event: asyncio.Event) -> None:
                stop_event.set()

        with (
            patch("app.main.signal.SIGINT", None),
            patch("app.main.signal.SIGTERM", None),
            patch("app.main.BinanceAggTradeReceiver", StoppingReceiver),
            patch("app.main.WebhookWorker", _WaitingWebhook),
        ):
            await run_worker(_config())

    async def test_unexpected_receiver_completion_fails_worker(self) -> None:
        class CompletedReceiver:
            def __init__(self, **_kwargs) -> None:
                pass

            async def run(self, _stop_event: asyncio.Event) -> None:
                return

        with (
            patch("app.main.signal.SIGINT", None),
            patch("app.main.signal.SIGTERM", None),
            patch("app.main.BinanceAggTradeReceiver", CompletedReceiver),
            patch("app.main.WebhookWorker", _WaitingWebhook),
        ):
            with self.assertRaises(RuntimeError) as raised:
                await run_worker(_config())

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)
        self.assertIn(
            "market task stopped unexpectedly",
            str(raised.exception.__cause__),
        )

    async def test_webhook_failure_stops_market_and_surfaces_cause(self) -> None:
        class WaitingReceiver:
            def __init__(self, **_kwargs) -> None:
                pass

            async def run(self, stop_event: asyncio.Event) -> None:
                await stop_event.wait()

        class FailingWebhook:
            def __init__(self, *_args, **_kwargs) -> None:
                pass

            async def run(self) -> None:
                raise ValueError("webhook crashed")

        with (
            patch("app.main.signal.SIGINT", None),
            patch("app.main.signal.SIGTERM", None),
            patch("app.main.BinanceAggTradeReceiver", WaitingReceiver),
            patch("app.main.WebhookWorker", FailingWebhook),
        ):
            with self.assertRaises(RuntimeError) as raised:
                await run_worker(_config())

        self.assertIsInstance(raised.exception.__cause__, ValueError)

    async def test_graceful_stop_drains_queued_alert(self) -> None:
        delivered = []

        class TriggeringReceiver:
            def __init__(self, *, observer, **_kwargs) -> None:
                self.observer = observer

            async def run(self, stop_event: asyncio.Event) -> None:
                self.observer.observe("BTCUSDT", 20_000, 1_000)
                self.observer.observe("BTCUSDT", 20_400, 2_000)
                stop_event.set()

        class CapturingWebhook:
            def __init__(self, _config, queue) -> None:
                self.queue = queue

            async def run(self) -> None:
                alert = await self.queue.get()
                delivered.append(alert)
                self.queue.task_done()
                await asyncio.Event().wait()

        with (
            patch("app.main.signal.SIGINT", None),
            patch("app.main.signal.SIGTERM", None),
            patch("app.main.BinanceAggTradeReceiver", TriggeringReceiver),
            patch("app.main.WebhookWorker", CapturingWebhook),
        ):
            await run_worker(_config())

        self.assertEqual(len(delivered), 1)
        self.assertEqual(delivered[0].price, 20_400)

if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import unittest

from app.main import _background_failure


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


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from queue_wakeup import RedisRunWakeup, RedisWakeupSettings


class _Pipeline:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fail = fail

    def lpush(self, *arguments: Any) -> _Pipeline:
        self.calls.append(("lpush", arguments))
        return self

    def ltrim(self, *arguments: Any) -> _Pipeline:
        self.calls.append(("ltrim", arguments))
        return self

    def expire(self, *arguments: Any) -> _Pipeline:
        self.calls.append(("expire", arguments))
        return self

    async def execute(self) -> list[object]:
        if self.fail:
            raise RedisConnectionError("unavailable")
        return []


class _Redis:
    def __init__(self, *, fail_wait: bool = False, fail_publish: bool = False) -> None:
        self.pipeline_value = _Pipeline(fail=fail_publish)
        self.fail_wait = fail_wait
        self.closed = False
        self.wait_arguments: tuple[list[str], int] | None = None

    def pipeline(self, *, transaction: bool) -> _Pipeline:
        assert transaction is True
        return self.pipeline_value

    async def blpop(self, keys: list[str], timeout: int) -> object:  # noqa: ASYNC109
        self.wait_arguments = (keys, timeout)
        if self.fail_wait:
            raise RedisConnectionError("unavailable")
        return None

    async def ping(self) -> bool:
        if self.fail_wait:
            raise RedisConnectionError("unavailable")
        return True

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_publish_is_bounded_and_wait_consumes_one_hint() -> None:
    client = _Redis()
    wakeup = RedisRunWakeup(cast("Any", client), key="agent:test", retention_seconds=60)
    run_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

    await wakeup.publish(run_id)
    await wakeup.wait(0.1)

    assert client.pipeline_value.calls == [
        ("lpush", ("agent:test", run_id.hex.encode("ascii"))),
        ("ltrim", ("agent:test", 0, 9999)),
        ("expire", ("agent:test", 60)),
    ]
    assert client.wait_arguments == (["agent:test"], 1)


@pytest.mark.asyncio
async def test_wait_falls_back_to_poll_delay_and_readiness_fails_closed() -> None:
    delays: list[float] = []

    async def delay(seconds: float) -> None:
        delays.append(seconds)

    client = _Redis(fail_wait=True)
    wakeup = RedisRunWakeup(
        cast("Any", client),
        key="agent:test",
        retention_seconds=60,
        fallback_sleep=delay,
    )

    await wakeup.wait(0.25)

    assert delays == [0.25]
    assert await wakeup.ready() is False


@pytest.mark.asyncio
async def test_publish_failure_falls_back_to_polling_and_owned_client_closes_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _Redis(fail_publish=True)
    wakeup = RedisRunWakeup(
        cast("Any", client),
        key="agent:test",
        retention_seconds=60,
        owns_client=True,
    )

    await wakeup.publish(uuid.uuid4())
    assert "polling will recover" in caplog.text
    await wakeup.aclose()
    await wakeup.aclose()

    assert client.closed is True
    assert await wakeup.ready() is False
    with pytest.raises(RuntimeError, match="closed"):
        await wakeup.wait(1)


def test_wakeup_settings_and_constructor_reject_unsafe_values() -> None:
    settings = RedisWakeupSettings(url="redis://redis:6379/1", retention_seconds=60)
    assert settings.url.get_secret_value() == "redis://redis:6379/1"

    client = _Redis()
    with pytest.raises(ValueError, match="retention"):
        RedisRunWakeup(cast("Any", client), key="agent:test", retention_seconds=1)

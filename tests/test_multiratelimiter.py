from datetime import datetime
import time

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from ratelimitx import MultiRateLimiter, RateLimiter, RateLimitError


@pytest_asyncio.fixture
async def client():
    client = Redis()
    await client.flushdb()
    return client


def test_from_mapping(client):
    mapping = {1: 1, 60: 2, 3_600: 3, 86_400: 5}

    multilimiter = MultiRateLimiter.from_mapping(
        client=client, identifier="test", mapping=mapping
    )

    assert len(multilimiter.rate_limiters) == 4

    limiter_one = RateLimiter(client=client, identifier="test", duration=1, n=1)
    assert multilimiter.rate_limiters[0] == limiter_one

    limiter_two = RateLimiter(client=client, identifier="test", duration=60, n=2)
    assert multilimiter.rate_limiters[1] == limiter_two

    limiter_three = RateLimiter(client=client, identifier="test", duration=3600, n=3)
    assert multilimiter.rate_limiters[2] == limiter_three

    limiter_four = RateLimiter(client=client, identifier="test", duration=86_400, n=5)
    assert multilimiter.rate_limiters[3] == limiter_four


def test_new_one(client):
    multilimiter = MultiRateLimiter.new(
        client=client, identifier="test", per_second=1, per_minute=2
    )

    assert len(multilimiter.rate_limiters) == 2

    second_limiter = RateLimiter(client=client, identifier="test", duration=1, n=1)
    assert multilimiter.rate_limiters[0] == second_limiter

    minute_limiter = RateLimiter(client=client, identifier="test", duration=60, n=2)
    assert multilimiter.rate_limiters[1] == minute_limiter


def test_new_two(client):
    multilimiter = MultiRateLimiter.new(
        client=client, identifier="test", per_hour=3, per_day=5
    )

    assert len(multilimiter.rate_limiters) == 2

    hour_limiter = RateLimiter(client=client, identifier="test", duration=3_600, n=3)
    assert multilimiter.rate_limiters[0] == hour_limiter

    day_limiter = RateLimiter(client=client, identifier="test", duration=86_400, n=5)
    assert multilimiter.rate_limiters[1] == day_limiter


def test_new_four(client):
    multilimiter = MultiRateLimiter.new(
        client=client, identifier="test", per_second=1, per_minute=2, per_hour=3, per_day=5
    )

    assert len(multilimiter.rate_limiters) == 4

    second_limiter = RateLimiter(client=client, identifier="test", duration=1, n=1)
    assert multilimiter.rate_limiters[0] == second_limiter

    minute_limiter = RateLimiter(client=client, identifier="test", duration=60, n=2)
    assert multilimiter.rate_limiters[1] == minute_limiter

    hour_limiter = RateLimiter(client=client, identifier="test", duration=3_600, n=3)
    assert multilimiter.rate_limiters[2] == hour_limiter

    day_limiter = RateLimiter(client=client, identifier="test", duration=86_400, n=5)
    assert multilimiter.rate_limiters[3] == day_limiter


@pytest.mark.asyncio
async def test_ratelimit(client):
    multilimiter = MultiRateLimiter.new(
        client=client, identifier="test", per_second=1, per_minute=3
    )

    """
        Think of a better way to test this.
    """

    start = time.time()
    now = start
    await multilimiter(now)

    # ~~~ half second later ~~~
    now = start + .5
    with pytest.raises(RateLimitError) as exc_info:
        await multilimiter(now)

    retry_after = exc_info.value.retry_after
    assert retry_after.seconds == .5
    assert retry_after.date == datetime.fromtimestamp(start + 1)

    # ~~~ Two minutes later ~~~
    now = start + 120
    await multilimiter(now)

    # ~~~ Two minutes and one second later ~~~
    now = start + 121
    await multilimiter(now)




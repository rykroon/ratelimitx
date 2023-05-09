from collections.abc import Iterable
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
        client=client, mapping=mapping
    )

    assert len(multilimiter.rate_limiters) == 4

    limiter_one = RateLimiter(client=client, window_length=1, n=1)
    assert multilimiter.rate_limiters[0] == limiter_one

    limiter_two = RateLimiter(client=client, window_length=60, n=2)
    assert multilimiter.rate_limiters[1] == limiter_two

    limiter_three = RateLimiter(client=client, window_length=3600, n=3)
    assert multilimiter.rate_limiters[2] == limiter_three

    limiter_four = RateLimiter(client=client, window_length=86_400, n=5)
    assert multilimiter.rate_limiters[3] == limiter_four


def test_new_one(client):
    multilimiter = MultiRateLimiter.new(
        client=client, per_second=1, per_minute=2
    )

    assert len(multilimiter.rate_limiters) == 2

    second_limiter = RateLimiter(client=client, window_length=1, n=1)
    assert multilimiter.rate_limiters[0] == second_limiter

    minute_limiter = RateLimiter(client=client, window_length=60, n=2)
    assert multilimiter.rate_limiters[1] == minute_limiter


def test_new_two(client):
    multilimiter = MultiRateLimiter.new(
        client=client, per_hour=3, per_day=5
    )

    assert len(multilimiter.rate_limiters) == 2

    hour_limiter = RateLimiter(client=client, window_length=3_600, n=3)
    assert multilimiter.rate_limiters[0] == hour_limiter

    day_limiter = RateLimiter(client=client, window_length=86_400, n=5)
    assert multilimiter.rate_limiters[1] == day_limiter


def test_new_four(client):
    multilimiter = MultiRateLimiter.new(
        client=client, per_second=1, per_minute=2, per_hour=3, per_day=5
    )

    assert len(multilimiter.rate_limiters) == 4

    second_limiter = RateLimiter(client=client, window_length=1, n=1)
    assert multilimiter.rate_limiters[0] == second_limiter

    minute_limiter = RateLimiter(client=client, window_length=60, n=2)
    assert multilimiter.rate_limiters[1] == minute_limiter

    hour_limiter = RateLimiter(client=client, window_length=3_600, n=3)
    assert multilimiter.rate_limiters[2] == hour_limiter

    day_limiter = RateLimiter(client=client, window_length=86_400, n=5)
    assert multilimiter.rate_limiters[3] == day_limiter


def timsim(duration_secs: int, step: float) -> Iterable[tuple[float, float]]:
    """
        Time Simulator (timsim)
        A generator that simulates time.
        duration_secs: The total duration to be simulated in seconds.
        step: The amount of time that passes per iteration.
        returns: A tuple containing the timestamp and the elapsed time.
    """
    elapsed_time = 0
    start_time = time.time()
    while elapsed_time <= duration_secs:
        yield start_time + elapsed_time, elapsed_time
        elapsed_time += step


@pytest.mark.asyncio
async def test_rate_limit(client):
    multilimiter = MultiRateLimiter.new(
        client=client, per_second=1, per_minute=6
    )

    # simulate every half second in the span of two minutes.
    for ts, time_elapsed in timsim(120, .5):

        # add some valid requests.
        if time_elapsed in (0, 2, 4, 6, 8, 10, 60, 65, 70, 75, 80, 85):
            await multilimiter.rate_limit("test", ts)

        # ~~~ Test for specific scenarios that will be rate limited. ~~~

        if time_elapsed == 0.5:
            # Test seconds rate limiter
            with pytest.raises(RateLimitError) as exc_info:
                await multilimiter.rate_limit("test", ts)
            
            retry_after = exc_info.value.retry_after
            assert retry_after.seconds == .5
            assert retry_after.date == datetime.fromtimestamp(ts + .5)

        if time_elapsed == 10.5:
            # This will be stopped by both rate limiters.
            # make sure that the retry after with the max date
            # is returned.
            with pytest.raises(RateLimitError) as exc_info:
                await multilimiter.rate_limit("test", ts)
            
            retry_after = exc_info.value.retry_after
            assert retry_after.seconds == 49.5
            assert retry_after.date == datetime.fromtimestamp(ts + 49.5)
        
        if time_elapsed == 11:
            with pytest.raises(RateLimitError) as exc_info:
                await multilimiter.rate_limit("test", ts)

            retry_after = exc_info.value.retry_after
            assert retry_after.seconds == 49
            assert retry_after.date == datetime.fromtimestamp(ts + 49)

        if time_elapsed == 60.5:
            with pytest.raises(RateLimitError) as exc_info:
                await multilimiter.rate_limit("test", ts)

            retry_after = exc_info.value.retry_after
            assert retry_after.seconds == 1.5
            assert retry_after.date == datetime.fromtimestamp(ts + 1.5)

        if time_elapsed == 70.5:
            with pytest.raises(RateLimitError) as exc_info:
                await multilimiter.rate_limit("test", ts)

            retry_after = exc_info.value.retry_after
            assert retry_after.seconds == 0.5
            assert retry_after.date == datetime.fromtimestamp(ts + 0.5)

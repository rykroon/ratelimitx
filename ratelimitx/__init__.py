import asyncio
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
import time

from redis.asyncio import Redis


@dataclass(order=True)
class RetryAfter:
    date: datetime
    seconds: float


@dataclass
class RateLimitError(Exception):
    retry_after: RetryAfter


@dataclass
class RateLimiter:
    client: Redis
    identifier: str
    window_length: int
    n: int
    prefix: str = "ratelimitx"
    delimiter: str = "|"

    @cached_property
    def key(self) -> str:
        """
        Returns the key that will be used to store the data into Redis.
        """
        return self.delimiter.join(
            [self.prefix, self.identifier, str(self.window_length)]
        )

    async def __call__(
        self, timestamp: float | None = None, add_timestamp: bool = True
    ):
        """
        Raise a RateLimitError if rate limited.
        """
        if timestamp is None:
            timestamp = time.time()

        count, least_recent_timestamp = await self.slide_window(timestamp)
        if count >= self.n:
            seconds = self.window_length - (timestamp - least_recent_timestamp)
            date = datetime.fromtimestamp(timestamp + seconds)
            retry_after = RetryAfter(date=date, seconds=seconds)
            raise RateLimitError(retry_after)

        if add_timestamp:
            await self.add_timestamp(timestamp)

    async def slide_window(
        self, timestamp: float | None = None
    ) -> tuple[int, float | None]:
        """
        Remove expired timestamps (slides the window).
        Returns a tuple containing the number of unexpired timestamps
        and the least recent timestamp if applicable or None.
        """
        if timestamp is None:
            timestamp = time.time()

        pipeline = self.client.pipeline()

        # Remove expired timestamps.
        pipeline.zremrangebyscore(self.key, min=0, max=timestamp - self.window_length)

        # Count the amount of timestamps remaining.
        pipeline.zcount(self.key, min=0, max=timestamp)

        # Get the lowest ranked score. (least recent timestamp)
        pipeline.zrange(self.key, start=0, end=0)

        _, count, scores = await pipeline.execute()
        return count, float(scores[0]) if scores else None

    async def add_timestamp(self, timestamp: float | None = None):
        """
        Adds a timestamp and updates the expiration.
        """
        if timestamp is None:
            timestamp = time.time()

        pipeline = self.client.pipeline()
        pipeline.zadd(self.key, {timestamp: timestamp})
        pipeline.expire(self.key, self.window_length)
        await pipeline.execute()


@dataclass
class MultiRateLimiter:
    rate_limiters: list[RateLimiter]

    @classmethod
    def from_mapping(cls, client: Redis, identifier: str, mapping: dict[int, int]):
        """
        Creates a new multi-ratelimiter by creating new rate limiter objects
        from a mapping containing window lengths for keys and units for values.
        """
        rate_limiters = [
            RateLimiter(client, identifier, window_length, n)
            for window_length, n in mapping.items()
        ]
        return cls(rate_limiters)

    @classmethod
    def new(
        cls,
        client: Redis,
        identifier: str,
        per_second: int | None = None,
        per_minute: int | None = None,
        per_hour: int | None = None,
        per_day: int | None = None,
    ):
        """
        Creates a new multi-ratelimiter by specifying the number
        of units for each unit of time.
        """
        assert any([per_second, per_minute, per_hour, per_day])
        mapping = {}
        if per_second is not None:
            mapping[1] = per_second

        if per_minute is not None:
            mapping[60] = per_minute

        if per_hour is not None:
            mapping[3600] = per_hour

        if per_day is not None:
            mapping[86400] = per_day

        return cls.from_mapping(client, identifier, mapping)

    async def __call__(self, timestamp: float | None = None):
        if timestamp is None:
            timestamp = time.time()

        # Perform rate-limiting on each rate limiter.
        coroutines = [rl(timestamp, add_timestamp=False) for rl in self.rate_limiters]
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        errors = [result for result in results if isinstance(result, RateLimitError)]
        if errors:
            # If there are any errors then find the max retry after
            # and raise a RateLimitError.
            retry_after = max(error.retry_after for error in errors)
            raise RateLimitError(retry_after)

        # Add timestamps.
        coroutines = [rl.add_timestamp(timestamp) for rl in self.rate_limiters]
        await asyncio.gather(*coroutines)

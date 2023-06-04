import asyncio
from dataclasses import dataclass, InitVar
from datetime import datetime
import time
from typing import ClassVar, Optional

from redis.asyncio import Redis


@dataclass(order=True)
class RetryAfter:
    # A date after which to retry.
    date: datetime
    # The number of seconds to wait before retrying.
    seconds: float


@dataclass
class RateLimitError(Exception):
    retry_after: RetryAfter


@dataclass
class RateLimiter:
    default_client: ClassVar[Optional[Redis]] = None

    window_length: int
    n: int
    prefix: Optional[str] = None
    delimiter: str = "|"
    client: InitVar[Optional[Redis]] = None

    def __init_subclass__(cls, /, client: Redis | None = None, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.default_client = client

    def __post_init__(self, client):
        self.client = client if client is not None else self.__class__.default_client
        if self.client is None:
            raise TypeError("Missing required argument: 'client'.")

    def _build_key(self, identifier: str) -> str:
        if self.prefix is None:
            return self.delimiter.join([identifier, str(self.window_length)])
        return self.delimiter.join([self.prefix, identifier, str(self.window_length)])

    async def rate_limit(
        self,
        identifier: str,
        timestamp: Optional[float] = None,
        add_timestamp: bool = True,
    ):
        """
        identifier: An identifier for the subject that is being rate limited.
        timestamp: The time at which the rate-limited event is happening. Defaults to
            the current system time.
        add_timestamp: If True, the timestamp will be added to the window if the subject
            was not rate-limited.

        Raises a RateLimitError if the event was rate limited.
        """
        if timestamp is None:
            timestamp = time.time()

        count, least_recent_timestamp = await self.slide_window(identifier, timestamp)
        if count >= self.n:
            seconds = self.window_length - (timestamp - least_recent_timestamp)
            date = datetime.fromtimestamp(timestamp + seconds)
            retry_after = RetryAfter(date=date, seconds=seconds)
            raise RateLimitError(retry_after)

        if add_timestamp:
            await self.add_timestamp(identifier, timestamp)

    async def slide_window(
        self, identifier: str, timestamp: Optional[float] = None
    ) -> tuple[int, Optional[float]]:
        """
        identifier: An identifier for the subject that is being rate limited.
        timestamp: The time at which the rate-limited event is happening. Defaults to
            the current system time.

        Removes expired timestamps (slides the window).
        Returns a tuple containing the number of unexpired timestamps
        and the least recent timestamp if applicable or None.
        """
        key = self._build_key(identifier)
        if timestamp is None:
            timestamp = time.time()

        pipeline = self.client.pipeline()

        # Remove expired timestamps.
        pipeline.zremrangebyscore(key, min=0, max=timestamp - self.window_length)

        # Count the amount of timestamps remaining.
        pipeline.zcount(key, min=0, max=timestamp)

        # Get the lowest ranked score. (least recent timestamp)
        pipeline.zrange(key, start=0, end=0)

        _, count, scores = await pipeline.execute()
        return count, float(scores[0]) if scores else None

    async def add_timestamp(self, identifier: str, timestamp: Optional[float] = None):
        """
        Adds a timestamp to the window and updates the TTL.
        """
        key = self._build_key(identifier)
        if timestamp is None:
            timestamp = time.time()

        pipeline = self.client.pipeline()
        pipeline.zadd(key, {timestamp: timestamp})
        pipeline.expire(key, self.window_length)
        await pipeline.execute()


@dataclass
class MultiRateLimiter:
    rate_limiters: list[RateLimiter]

    @classmethod
    def from_mapping(cls, client: Redis, mapping: dict[int, int]):
        """
        Creates a new multi-ratelimiter by creating new rate limiter objects
        from a mapping containing window lengths for keys and units for values.
        """
        rate_limiters = [
            RateLimiter(client, window_length, n)
            for window_length, n in mapping.items()
        ]
        return cls(rate_limiters)

    @classmethod
    def new(
        cls,
        client: Redis,
        per_second: Optional[int] = None,
        per_minute: Optional[int] = None,
        per_hour: Optional[int] = None,
        per_day: Optional[int] = None,
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

        return cls.from_mapping(client, mapping)

    async def rate_limit(self, identifier: str, timestamp: Optional[float] = None):
        if timestamp is None:
            timestamp = time.time()

        # Perform rate-limiting on each rate limiter.
        coroutines = [
            rl.rate_limit(identifier, timestamp, add_timestamp=False)
            for rl in self.rate_limiters
        ]
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        errors = [result for result in results if isinstance(result, RateLimitError)]
        if errors:
            # If there are any errors then find the max retry after
            # and raise a RateLimitError.
            retry_after = max(error.retry_after for error in errors)
            raise RateLimitError(retry_after)

        # Add timestamps.
        coroutines = [
            rl.add_timestamp(identifier, timestamp) for rl in self.rate_limiters
        ]
        await asyncio.gather(*coroutines)

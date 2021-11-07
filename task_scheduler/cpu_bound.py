from typing import Callable, Iterable, Iterator, Any, Optional
from concurrent.futures import ProcessPoolExecutor, Future, Executor


class CPUBoundTaskScheduler(Executor):
    def __init__(self, *, max_workers, **kwargs):
        self._pool = ProcessPoolExecutor(max_workers=max_workers, **kwargs)

    def submit(*args: Any, **kwargs: Any) -> Future:
        self, fn, *args = args
        return self._pool.submit(fn, *args, **kwargs)

    def map(
        self, fn: Callable,
            *iterables: Iterable[Any],
            timeout: Optional[float] = 10,
    ) -> Iterator:
        return self._pool.map(fn, *iterables, timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

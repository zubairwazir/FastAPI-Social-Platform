import unittest
from task_scheduler.cpu_bound import CPUBoundTaskScheduler


def fn(n: int):
    return n ** 2


class CPUBoundSchedulerTaskTest(unittest.TestCase):
    def test_submit(self):
        executor = CPUBoundTaskScheduler(max_workers=2)

        result = executor.submit(fn, 3)
        self.assertEqual(result.result(timeout=10), 9)

        executor.shutdown()

    def test_map(self):
        executor = CPUBoundTaskScheduler(max_workers=4)
        args = list(range(1, 5))

        result = list(executor.map(fn, args))
        self.assertListEqual(result, [1, 4, 9, 16])

        executor.shutdown()

import time
import ray


def select_wait(num_task, num_returns=None, timeout=None, step=1, select=None):
    """
    A user supplied select function decides/generates
    what tasks to add to the window.
    """
    start = time.time()
    ready, not_ready = ray.wait(select([]), num_returns=step)
    num_ready, global_ready = len(ready), ready

    while len(global_ready) < num_task:
        print("sched select: ready tasks", ready)
        if (num_returns is not None and num_ready >= num_returns) or \
                (timeout is not None and (time.time() - start) > timeout):
            return global_ready, not_ready

        not_ready += select(ready)
        print("sched select: current running queue size", len(not_ready))

        ready, not_ready = ray.wait(not_ready, num_returns=step)
        global_ready += ready

        num_ready += step
        print("sched select: done %d/%d tasks" % (num_ready, num_task))

    return global_ready, not_ready


def slide_wait(tasks, num_returns=None, timeout=None, window_size=1, step=1):
    """Emulating a sliding window execution of tasks in the task queue.

    tasks: the task queue.
    num_returns: same as ray.wait()'s.
    timeout: same as ray.wait()'s.
    window_size: the number of pending tasks.
    step: the number of tasks to finish to slide.
    """

    start, cur_pos, end_pos = time.time(), window_size, len(tasks)
    global_ready, not_ready = ray.wait(tasks[:min(window_size, len(tasks))],
                                       num_returns=step)
    num_ready = 1

    while len(not_ready) > 0:
        if (num_returns is not None and num_ready >= num_returns) or \
                (timeout is not None and (time.time() - start) > timeout):
            return global_ready, not_ready

        if cur_pos < end_pos:
            not_ready.append(tasks[cur_pos])
        ready, not_ready = ray.wait(not_ready, num_returns=step)
        global_ready += ready
        cur_pos += step

        # accounting
        num_ready += step
        print("sched slide: done %d/%d tasks" % (num_ready, end_pos))

    return global_ready, not_ready
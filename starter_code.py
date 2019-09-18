import mock_db
import uuid
from worker import worker_main
from threading import Thread
import time
import logging as log


root = log.getLogger()
root.setLevel(log.DEBUG)


# So essentially, we need to add worker jobs to a queue in mock_db,
# Worker created: add to end of queue
# lock_is_free should check if you are at the beginning of the queue. false otherwise
#    - generally though, you would want multiple workers to be able to run concurrently?
#        - I guess that's a different problem
# when a worker job is done, or crashed, it should be removed from the queue
#    - should add a status message in mock_db indicating if the job failed or not


#
# WHOOPS, can't use threading primitives, as these workers would actually be on many different machines in practice

# global_lock = Lock()
#
#
#
# def single_threaded(func):
#     """
#     Decorator that surrounds a method with a blocking lock
#
#     :param func: the func to run single threaded
#     """
#
#     single_threaded_lock = Lock()
#     log.debug(f"single_threaded_lock for func: {func}")
#
#     def decorator(*args, **kwargs):
#         single_threaded_lock.acquire()
#         log.debug(f"got lock, worker_hash: {get_ident()} for func: {func}")
#         try:
#             ret_val = func(*args, **kwargs)
#         finally:
#             single_threaded_lock.release()
#             log.debug(f"released lock, worker_hash: {get_ident()} for func: {func}")
#
#         return ret_val
#
#     return decorator
#
#
# @single_threaded
def lock_is_free(worker_hash, db):
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """

    return True


def retry(func, give_up_after, retry_interval):

    def decorator():
        pass


    return decorator

def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    """
        CHANGE MY IMPLEMENTATION, BUT NOT FUNCTION SIGNATURE

        Run the worker from worker.py by calling worker_main

        Args:
            worker_hash: a random string we will use as an id for the running worker
            give_up_after: if the worker has not run after this many seconds, give up
            db: an instance of MockDB
            retry_interval: continually poll the locking system after this many seconds
                            until the lock is free, unless we have been trying for more
                            than give_up_after seconds
    """

    current_time = 0
    while current_time < give_up_after:
        try:
            if lock_is_free():
                worker_main(worker_hash, db)
                return
        except Exception as e:
            log.exception(f"Error occurred in worker: `{worker_hash}`. Retrying after {retry_interval} seconds.", e)
            pass

        log.info(f"{worker_hash} crashed: retrying after {retry_interval} seconds")
        current_time += retry_interval
        time.sleep(retry_interval)

    log.info(f"Timeout reached for worker: {worker_hash}, giving up")


if __name__ == "__main__":
    """
        DO NOT MODIFY

        Main function that runs the worker five times, each on a new thread
        We have provided hard-coded values for how often the worker should retry
        grabbing lock and when it should give up. Use these as you see fit, but
        you should not need to change them
    """

    db = mock_db.DB()
    threads = []
    for _ in range(25):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 0.1))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

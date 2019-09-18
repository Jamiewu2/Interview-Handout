from typing import List

import mock_db
import uuid
from worker import worker_main
from threading import Thread
import time
import logging as log
from dataclasses import dataclass, asdict, field
from enum import Enum
from mashumaro import DataClassDictMixin

root = log.getLogger()
root.setLevel(log.DEBUG)

class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class Job(DataClassDictMixin):
    """DB Representation of a Job"""
    # todo add some metrics, like job submit time, job run time
    _id: str
    status: JobStatus = JobStatus.PENDING
    error_message: str = None  # if a job fails, put the error message here


@dataclass
class JobQueue(DataClassDictMixin):
    _id: str = "job_queue"
    jobs: List[Job] = field(default_factory=list)
    version: int = 1  # if we get wayyyyy too many jobs this will overflow

    @staticmethod
    def job_queue_key():
        return {"_id": "job_queue"}


# So essentially, we need to add worker jobs to a queue in mock_db,
# Worker created: add to end of queue
# lock_is_free should check if you are at the beginning of the queue. false otherwise
#    - generally though, you would want multiple workers to be able to run concurrently?
#        - I guess that's a different problem
# when a worker job is done, or crashed, it should be removed from the queue
#    - should add a status message in mock_db indicating if the job failed or not

# so as it turns out, keeping a queue seems really hard without some database-side concurrency locks
# we can just submit, and consume jobs "at random". This might lead to starvation, but all the jobs have to run anyway
# this would assume db.find is deterministic

def add_to_queue(db, worker_hash):
    job = Job(worker_hash)
    job_queue_key = JobQueue.job_queue_key()

    job_queue_dict = db.find_one(job_queue_key)

    if not job_queue_dict:
        job_queue = JobQueue()
        job_queue.jobs.append(job)

        db.insert_one(job_queue.to_dict())
    else:
        job_queue = JobQueue.from_dict(job_queue_dict)
        job_queue.jobs.append(job)

        db.update_one(job_queue_key, job_queue.to_dict())


#todo hide, should use updates instead
def remove_from_queue(db, worker_hash, status):
    # db.delete_one({"_id": worker_hash})

    pass


def update_job_failed(db, job, error_message):
    pass


def lock_is_free():
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """

    return True


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

    add_to_queue(db, worker_hash)

    print(db.store)
    time.sleep(1)
    remove_from_queue(db, worker_hash, JobStatus.SUCCESS)

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

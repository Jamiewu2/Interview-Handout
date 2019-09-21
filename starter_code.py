from typing import List

import mock_db
import uuid
from worker import worker_main
from threading import Thread, get_ident
import time
import logging as log
from dataclasses import dataclass, asdict, field
from enum import Enum
from mashumaro import DataClassDictMixin

root = log.getLogger()
root.setLevel(log.INFO)


# So essentially, we need to add worker jobs to a queue in mock_db,
# Worker created: add to end of queue
# lock_is_free should check if you are at the beginning of the queue. false otherwise
#    - generally though, you would want multiple workers to be able to run concurrently?
#        - I guess that's a different problem
# when a worker job is done, or crashed, it should be removed from the queue
#    - should add a status message in mock_db indicating if the job failed or not
#    - Assumption: currently attempting to restart jobs that crash

# lock idea, blindly attempt to create lock field, if it exists, you'll get DuplicateKeyError
# delete lock field when done

# current implementation works, but the entire queue is just one item

# INITIAL IDEA
# alternatively, I could submit all jobs as separate documents with a timestamp
# when checking if lock_is_free, use a find operation to get the oldest document that is still `JobStatus.PENDING`
# my concern is that time across multiple machines (as the jobs would be in a real-world example) could have off sync clocks
# meaning a job could possibly be inserted into the front of the queue after a job has started running
#     - unless you lock both the find operation, and the insert operation under the same lock, which kind of defeats the purpose of this design
# thus, adding a timestamp would have to be implemented database side I would think
# though, since this code snippet is all running on a single machine, it would probably work


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

    @staticmethod
    def job_queue_key():
        return {"_id": "job_queue"}


def parametrized(dec):
    """
    Meta-decorator that allows you to have decorators with parameters
    :param dec:
    :return:
    """
    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)
        return repl
    return layer


@parametrized
def single_threaded(func, lock_key):
    """
    lock idea, blindly attempt to create lock field, if it exists, you'll get DuplicateKeyError
    delete lock field when done

    Pessimistic Locking Decorator. Use to ensure only a function is called only once
    Make sure you don't attempt to lock the same lock within that function, will lead to deadlock

    Args:
        lock_key: the keyword to use to lock a database transaction
    """
    def decorator(*args, **kwargs):
        db = args[0]  # todo hack, assume db is first arg

        while 1:
            try:
                db.insert_one({'_id': lock_key})
                log.debug(f"thread: {get_ident()} got in")
                break
            except Exception as e:
                #todo retry until timeout
                time.sleep(0.1)

        try:
            func(*args, **kwargs)
        finally:
            db.delete_one({'_id': lock_key})

    return decorator


DATABASE_LOCK_KEY = "database_lock_key"


@single_threaded(DATABASE_LOCK_KEY)
def init_queue(db):
    """
    Optimally, would just create the queue in a pre-process step
    :param db:
    :return:
    """
    job_queue_key = JobQueue.job_queue_key()
    job_queue_dict = db.find_one(job_queue_key)

    if not job_queue_dict:
        job_queue = JobQueue()
        db.insert_one(job_queue.to_dict())


def _get_queue_from_db(db) -> JobQueue:
    """
    :param db: mock_db
    :return: the job queue from the db, or raise an exception is the job queue is not initialized
    """
    job_queue_key = JobQueue.job_queue_key()
    job_queue_dict = db.find_one(job_queue_key)
    if not job_queue_dict:
        raise Exception("job queue not initialized")

    return JobQueue.from_dict(job_queue_dict)


@single_threaded(DATABASE_LOCK_KEY)
def add_to_queue(db, worker_hash):
    job = Job(worker_hash)
    job_queue = _get_queue_from_db(db)
    job_queue_key = JobQueue.job_queue_key()

    job_queue.jobs.append(job)
    db.update_one(job_queue_key, job_queue.to_dict())


#todo should use updates instead, use job_status
@single_threaded(DATABASE_LOCK_KEY)
def remove_from_queue(db, worker_hash, job_status):
    job = Job(worker_hash)
    job_queue = _get_queue_from_db(db)
    job_queue_key = JobQueue.job_queue_key()

    job_queue.jobs.remove(job)
    db.update_one(job_queue_key, job_queue.to_dict())


def update_job_failed(db, job, error_message):
    pass


def lock_is_free(db, worker_hash):
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """

    job_queue = _get_queue_from_db(db)
    if job_queue.jobs[0]._id == worker_hash:
        return True

    return False


def write_line(file_name, line):
    """
        Function to write the provided text to the provided file in append mode

        Args:
            file_name: the file to which to write the text
            line: text to write to the file
    """

    with open(file_name, 'a') as f:
        f.write(line)


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

    log.info("start")

    init_queue(db)
    add_to_queue(db, worker_hash)

    num_jobs = len(db.store['job_queue']['jobs'])
    log.info(f"added job: {worker_hash}, currently {num_jobs} in queue")

    current_time = 0
    while current_time < give_up_after:
        try:
            if lock_is_free(db, worker_hash):
                log.info(f"running job: {worker_hash}")
                worker_main(worker_hash, db)
                remove_from_queue(db, worker_hash, JobStatus.SUCCESS)
                write_line("output.txt", "")
                return
        except Exception as e:
            log.exception(f"Error occurred in worker: `{worker_hash}`, retrying after {retry_interval} seconds", e)
            remove_from_queue(db, worker_hash, JobStatus.FAILED)
            write_line("output.txt", "")
            return


        log.debug(f"{worker_hash}: retrying after {retry_interval} seconds")
        current_time += retry_interval
        time.sleep(retry_interval)

    log.info(f"Timeout reached for worker: {worker_hash}, giving up")
    remove_from_queue(db, worker_hash, JobStatus.FAILED)
    write_line("output.txt", "")


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

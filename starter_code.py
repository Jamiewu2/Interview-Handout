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
from datetime import datetime

root = log.getLogger()
root.setLevel(log.INFO)


# IDEA
# I would submit all jobs as separate documents with a timestamp
# when checking if lock_is_free, use a find operation to get the oldest document that is still `JobStatus.PENDING`
# my concern is that time across multiple machines (as the jobs would be in a real-world example) could have off sync clocks
# meaning a job could possibly be inserted into the front of the queue after a job has started running
#     - unless you lock both the find operation, and the insert operation under the same lock, which kind of defeats the purpose of this design
# thus, adding a timestamp would have to be implemented database side I would think
# though, since this code snippet is all running on a single machine, it would probably work

# lock idea, blindly attempt to create lock field, if it exists, you'll get DuplicateKeyError
# delete the lock field when done


class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class Job(DataClassDictMixin):
    """DB Representation of a Job"""
    _id: str
    job_submit_time: float
    job_run_time: float = None
    status: JobStatus = JobStatus.PENDING
    error_message: str = None  # if a job fails, put the error message here


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
            return func(*args, **kwargs)
        finally:
            db.delete_one({'_id': lock_key})

    return decorator


DATABASE_LOCK_KEY = "database_lock_key"


def _get_job_from_db(db, worker_hash) -> Job:
    """
    :param db:
    :param worker_hash:
    :return: get the job object from db, or raise exception if 'worker_hash' not found
    """
    job_dict = db.find_one({'_id': worker_hash})
    if not job_dict:
        raise Exception(f"job: {worker_hash} not found")
    return Job.from_dict(job_dict)


@single_threaded(DATABASE_LOCK_KEY)
def add_to_queue(db, worker_hash):
    timestamp = datetime.now().timestamp()
    job = Job(worker_hash, timestamp)

    # safety check
    first_job = _get_first_job_in_queue(db)
    if first_job and timestamp < first_job.job_submit_time:
        raise Exception("Attempting to add a job into the front of the queue")

    db.insert_one(job.to_dict())


def update_job(db, worker_hash, job_status, error_message=None):
    """
    Update the job in the db with a job status, and optionally an error message
    :param db:
    :param worker_hash:
    :param job_status:
    :param error_message:
    :return:
    """
    job = _get_job_from_db(db, worker_hash)
    job.status = job_status
    job.job_run_time = datetime.now().timestamp() - job.job_submit_time
    if error_message:
        job.error_message = error_message

    db.update_one({'_id': job._id}, job.to_dict())


def _get_first_job_in_queue(db):
    """
    :param db:
    :return: Returns the first job in the queue, or None if the queue is empty
    """
    jobs = db.find_many({"status": JobStatus.PENDING.value})
    if not jobs:
        return None

    jobs = map(Job.from_dict, jobs)

    first_job = sorted(jobs, key=lambda job: job.job_submit_time)[0]
    return first_job


def lock_is_free(db, worker_hash):
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free
    """
    first_job = _get_first_job_in_queue(db)
    if first_job and first_job._id == worker_hash:
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

    add_to_queue(db, worker_hash)
    log.info(f"added: {worker_hash} to queue")

    current_time = 0
    while current_time < give_up_after:
        try:
            if lock_is_free(db, worker_hash):
                log.info(f"running job: {worker_hash}")
                worker_main(worker_hash, db)
                update_job(db, worker_hash, JobStatus.SUCCESS)
                write_line("output.txt", "")
                return

        except Exception as e:
            log.exception(f"Error occurred in worker: `{worker_hash}`.", e)
            update_job(db, worker_hash, JobStatus.FAILED, repr(e))
            write_line("output.txt", "")
            return

        log.debug(f"{worker_hash}: retrying after {retry_interval} seconds")
        current_time += retry_interval
        time.sleep(retry_interval)

    log.info(f"Timeout reached for worker: {worker_hash}, giving up")
    update_job(db, worker_hash, JobStatus.FAILED, "Timeout reached")
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

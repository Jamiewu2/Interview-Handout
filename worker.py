"""
    DO NOT MODIFY

    A simple worker that simulates the kind of task we run in the ETL
    In chunks, it will write some text to output.txt
    However, it may not be successful on every run
"""

from time import sleep
import random
import mock_db

text = 'Maestro is the best......\n\n'

def write_line(file_name, line):
    """
        Function to write the provided text to the provided file in append mode

        Args:
            file_name: the file to which to write the text
            line: text to write to the file
    """

    with open(file_name, 'a') as f:
        f.write(line)


def worker_main(worker_hash, db):
    """
        Main routine of this worker that crashes on some probability.
        Writes some text to output.txt in chunks and sleeps after each

        Args:
            worker_hash: a random string we will use as an id for the running worker
            db: an instance of MockDB
    """

    CRASH_PROBABILITY = 0.2
    should_crash = random.random()
    if should_crash < CRASH_PROBABILITY:
        raise Exception("Crash")

    CHUNK_SIZE = 5
    SLEEP_DURATION = 2
    cursor = 0
    while cursor < len(text):
        start = cursor
        end = min(cursor + CHUNK_SIZE, len(text))
        write_line('output.txt', text[start: end])
        sleep(SLEEP_DURATION)
        cursor += CHUNK_SIZE

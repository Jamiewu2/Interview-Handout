from time import sleep
import random
import mock_db

text = 'Maestro is the best......'

def write_line(file_name, line):
    with open(file_name, 'a') as file:
        file.write(line)

def worker_main(worker_hash, db, cb):
    print("In worker main")
    should_crash = random.random()
    if should_crash < 0.0:
        raise Exception("Crash")
    cursor = 0
    text_to_write = f'{text}\n\n'
    while cursor < len(text_to_write):
        start = cursor
        end = min(cursor + 5, len(text_to_write))
        write_line('./output.txt', text_to_write[start: end])
        sleep(2)
        cursor += 5
    cb()
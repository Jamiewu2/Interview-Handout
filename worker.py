from time import sleep
import random
import mock_db

text = 'Maestro is the best......\n\n'

def write_line(file_name, line):
    with open(file_name, 'a') as f:
        f.write(line)

def worker_main(worker_hash, db, cb):
    print("In worker main")
    should_crash = random.random()
    if should_crash < 0.5:
        raise Exception("Crash")
        
    cursor = 0   
    while cursor < len(text):
        start = cursor
        end = min(cursor + 5, len(text))
        write_line('output.txt', text[start: end])
        sleep(2)
        cursor += 5
    cb()

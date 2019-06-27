import mock_db
import uuid
from worker import worker_main
from threading import Thread

def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    def cb():
        pass
    worker_main(worker_hash, db, cb)

if __name__ == "__main__":
    db = mock_db.DB()
    threads = []
    print("START 5 THREADS")
    for _ in range(5):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 1))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
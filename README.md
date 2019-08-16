# InterviewHandout

### Situation: 
The ETL team at MaestroQA manages several python workers (i.e. functional blocks of code) that run at various intervals. For various reasons, we may not want more than one of the same job (i.e. instance of a worker with a defined payload) running for a given client at any given time. In other words, we may not want "worker.py" to run immediately for a client if it is already running for that client - instead waiting for the current job to finish before it begins.

### Goal: 
Implement a locking system using the database operations defined in mock_db.py, that can achieve the goal eluded to in the aforementioned situation above. We have provided a sample worker to use your solution, as well as a test script to verify the results of running the worker.

### Files:

In *starter_code.py*, we run worker.py 5 times on different threads. This is to simulate the queuing of worker jobs for a particular client [absent from the code]. This should be the only file you edit.

In *worker.py*, we have a simple python script that will write 'Maestro is the best......' to output.txt. This is to act as the worker we wish to run for a particular client.

In *test_output.py* we have a simple test to verify the correctness of the output. We want the text from above to be written several times, separated by 2 newlines each time. Note that this may be fewer than 5 times, as the workers are designed to crash with some probability. Note: Be sure to remove the contents of output.txt after an unsuccessful run, as this could impact subsequent runs of the test script.

In *mock_db.py* there are several functions that you can use in your code that will help simulate database calls similar to a real system. This file is only here to provide functionality and should not be edited.

### Note:

If you run the starter code, you will see that there is a concurrency issue, as multiple workers write to the file at the same time, interleaving the chunks of text. Using the fake database functions in mock_db.py, come up with a system to control the execution of workers so that the concurrency is handled correctly [this should be written in starter_code.py]. You will only need to modify code outside the main function in starter_code.py. Furthermore, this should be accomplished by running the script once. That is, it should handle all failures appropriately and run all subsequent workers. A valid solution will write the previously mentioned output to output.txt by only running `python start_code.py`. It will also pass all assertions when running `python test_output.py`.

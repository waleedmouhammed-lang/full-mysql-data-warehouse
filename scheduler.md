How to Run the ETL Scheduler as a Background Process

1. The "Problem": Why Does the Script "Hang"?

When you run python schedule_etl.py in your terminal, you see it run the first time and then "hang" or "freeze."

This is not an error! This is the script working as designed.

The script has a while True: loop at the end. Its job is to run forever, checking every second to see if it's time to run the 24-hour job again. This is called a foreground process—it runs in your active terminal and won't give you your prompt back until it's finished (which is never).

To run this like a real-world service, we need to run it as a background process.

2. The Solution: nohup and &

We will use a standard Unix command called nohup to run the script in the background. This will:

Run the script as a background "daemon" (service).

Prevent the script from being "hung up" (killed) when you close your terminal.

Redirect all console logs to a file so we can check them later.

3. Step-by-Step Instructions

Follow these steps to start your scheduler service.

Step 1: Open Your Terminal

Open a new terminal window.

Step 2: Navigate to Your Project

Go to your project directory.

# Example:
cd /Users/waleedmouhammed/Documents/GitHub/dw_dev


Step 3: Activate Your Virtual Environment

You must activate your dw-venv so that the script can find all the Python libraries (like schedule and pandas).

source dw-venv/bin/activate


Your prompt should now start with (dw-venv), showing that the environment is active.

Step 4: Run the Background Command

Now, run the following command. This is the main instruction:

nohup python etl_scheduler.py > scheduler.out 2>&1 &


Your terminal will show something like [1] 12345 (that's the Process ID) and then immediately give you your prompt back. The script is now running in the background.

Command Breakdown:

nohup: Stands for "No Hangup." Lets the script keep running even if you close the terminal.

python schedule_g.py: The command to run (using the python from your active venv).

> scheduler.out: Redirects all standard output (your logging.info messages) to a file named scheduler.out.

2>&1: Redirects standard error (any error messages or tracebacks) to the same place as the standard output (scheduler.out).

&: The ampersand & is the most important part—it tells the terminal to "run this command in the background."

4. How to Check Your Scheduler's Logs

Since the script is running in the background, you won't see its logs in your terminal. They are all being written to the scheduler.out file.

To watch the logs live, you can use the tail -f command in your terminal:

tail -f scheduler.out


This will "follow" the log file, and you will see new logs appear in your terminal as they are written. Press CTRL+C to stop watching (this only stops tail, not your script).

5. How to Stop Your Scheduler

The script will run forever until you manually stop it. To do this, you need to find its Process ID (PID) and then use the kill command.

Step 1: Find the Process ID (PID)

In your terminal, run this command:

ps aux | grep schedule_etl.py


ps aux: Lists all running processes.

| grep ...: "Pipes" that list into the grep command, which filters for lines containing schedule_etl.py.

You will see an output like this:

waleedmouhammed   12345   0.0  0.1  12345678  12345 s001  S+    1:24PM   0:00.10 python schedule_etl.py


The second number (here, 12345) is the PID.

Step 2: Kill the Process

Use the kill command with that PID.

# Use the PID you found in Step 1
kill 12345


This will terminate the background script. You can run ps aux | grep schedule_etl.py again to confirm that it's gone.
import threading

scheduler_thread = None
scheduler_running = False


def is_running():
    return scheduler_running


def set_running(value: bool):
    global scheduler_running
    scheduler_running = value


def set_thread(thread):
    global scheduler_thread
    scheduler_thread = thread


def get_thread():
    return scheduler_thread
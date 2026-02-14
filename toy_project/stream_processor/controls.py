import threading

# Global control flags
paused = threading.Event()
paused.set()  # Start unpaused (set = True means "go")
stop_event = threading.Event()

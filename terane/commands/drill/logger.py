
_buffer = []

def log(msg):
    global _buffer
    _buffer.append(msg)

def flush():
    global _buffer
    for msg in _buffer:
        print msg
    _buffer = []

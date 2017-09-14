from contextlib import contextmanager
import tempfile
import os
import json
import fcntl
from collections import OrderedDict

def load(path):
    with open(os.path.join(path, "metadata.json")) as f:
        return json.load(f)

def save(path, metadata):
    p = os.path.join(path, "metadata.json")
    with _atomic_write(p) as f:
        # http://www.psf.upfronthosting.co.za/issue25457
        data = OrderedDict(sorted(metadata.items(), key=str))
        json.dump(data,
                  f,
                  indent=4,
                  separators=(',', ': '))

@contextmanager
def update(path):
    """
    allow concurrent update of metadata
    """
    p = os.path.join(path, "metadata.json")
    # we have to open writeable to get a lock
    with open(p, "a") as f:
        fcntl.lockf(f, fcntl.LOCK_EX)
        data = load(path)
        yield(data)
        save(path, data)
        fcntl.lockf(f, fcntl.LOCK_UN)

@contextmanager
def _atomic_write(filename):
    path = os.path.dirname(filename)
    try:
        file = tempfile.NamedTemporaryFile(delete=False, dir=path, mode="w+")
        yield file
        file.flush()
        os.fsync(file.fileno())
        os.rename(file.name, filename)
    finally:
        try:
            os.remove(file.name)
        except OSError as e:
            if e.errno == 2:
                pass
            else:
                raise e
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        sys.stderr.write("USAGE: %s measurment...\n" % sys.argv[0])
        sys.exit(1)
    for arg in sys.argv[1:]:
        with update(arg) as m:
            pass

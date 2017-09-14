import socket
from ipyparallel import Client

rc = Client()
lview = rc.load_balanced_view()
ar = lview.apply_async(socket.gethostname)
pid_map = ar.get_dict()
print(pid_map)

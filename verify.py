from datetime import datetime, timedelta, time
from sys import argv

last_prac = int(argv[1]) 
hl = float(argv[2])

now = datetime.now().timestamp() * 1000

delta = (now - last_prac) / 1000.0 / 86400.0

print ("delta", delta)

recall = 2 ** (-delta / hl)

print ("recall", recall)

import string
import random
import datetime as dt

def run():
  while True:
    d       = {}
    d['t']  = int(dt.datetime.now().timestamp()*1000)
    d['id'] = ''.join(random.choice(string.ascii_uppercase) for _ in range(2))
    d['v']  = random.randint(1,99)
    yield d
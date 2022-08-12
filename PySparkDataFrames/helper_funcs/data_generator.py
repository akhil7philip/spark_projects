import string
import random
import datetime as dt

def run():
  while True:
    d       = {}
    d['t']  = int(dt.datetime.now().timestamp()*1000)
    d['id'] = random.choice(string.ascii_uppercase)
    d['v']  = random.randint(0,9)
    yield d
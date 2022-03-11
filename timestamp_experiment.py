from datetime import datetime
import time
# Getting the current date and time
dt = datetime.now()

# getting the timestamp
ts = datetime.timestamp(dt)

print("Date and time is:", dt)
print("Timestamp is:", ts)
time.sleep(10)
dt2 = datetime.now()

# getting the timestamp
ts2 = datetime.timestamp(dt2)

print("Date and time is:", dt2)
print("Timestamp is:", ts2)
print("diff: ", ts2 - ts)
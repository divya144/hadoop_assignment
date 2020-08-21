import sys
import random
import time
file1=open("in.txt","w")
start_time=time.time()
for i in range(0,10000000):
    tid=i+1
    file1.write(str(tid)+" ")
    no_of_items=random.randint(1,50)
    for j in range(0,no_of_items):
        price=random.randint(100,5000)
        if j==no_of_items-1:
            file1.write(str(price)+"\n")
        else:
            file1.write(str(price)+" ")
print(f"Total time taken = {time.time()-start_time} seconds")

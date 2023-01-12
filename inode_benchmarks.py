import base64
import sys
import numpy as np
import time
import random
import pdb

# memory usage of objects
inode = 1675344517869420558
print(f"Number used in tests:\t{inode}\n")

print("Single values:")
print(f"Memory usage as int:\t{sys.getsizeof(inode)}")
print(f"Memory usage as float:\t{sys.getsizeof(float(inode))}")
print(f"Memory usage as str:\t{sys.getsizeof(str(inode))}")
print(f"Memory usage as np.int:\t{sys.getsizeof(np.int(inode))}")
print(f"Memory usage as np.int64:\t{sys.getsizeof(np.int64(inode))}")
print(f"Memory usage as byte string:\t{sys.getsizeof(str(inode).encode())}")

print("\nList of values:")
print(f"Memory usage of empty list:\t{sys.getsizeof([])}")
print(f"Memory usage of 500*int list:\t{sys.getsizeof([inode]*500)}")
print(f"Memory usage of 500*float list:\t{sys.getsizeof([float(inode)]*500)}")
print(f"Memory usage of 500*np.int16 list:\t{sys.getsizeof(np.array([inode]*500, dtype=np.int16))} (list[0] = {np.array([inode], dtype=np.int16)[0]})")
print(f"Memory usage of 500*np.int32 list:\t{sys.getsizeof(np.array([inode]*500, dtype=np.int32))} (list[0] = {np.array([inode], dtype=np.int32)[0]})")
print(f"Memory usage of 500*np.int64 list:\t{sys.getsizeof(np.array([inode]*500, dtype=np.int64))} (list[0] = {np.array([inode], dtype=np.int64)[0]})")
print(f"Memory usage of 500*int set:\t{sys.getsizeof(set(list(range(inode, inode+500))))}")

print("\nLong lists of 0s and 1s:")
print(f"Memory usage (MiB) of [0]*10000000:\t{sys.getsizeof([0]*10000000)/1024**2}")
print(f"Memory usage (MiB) of np.array([0]*10000000):\t{sys.getsizeof(np.array([0]*10000000))/1024**2}")
print(f"Memory usage (MiB) of np.array([0]*10000000, dtype=np.int8):\t{sys.getsizeof(np.array([0]*10000000, dtype=np.int8))/1024**2}")

print("\nLookup times and memory of complex structures:")

# repeat 50000 times
inode_set = set(list(range(inode, inode+100000)))
t0 = time.time()
for i in range(50000):
    random.randint(inode, inode+100000) in inode_set
print(f"Set lookup time:\t{time.time()-t0}s, mem size {sys.getsizeof(inode_set)/1024**2} MiB")


# repeat 5000 times
inode_list = list(range(inode, inode+100000))
t0 = time.time()
for i in range(5000):
    random.randint(inode, inode+100000) in inode_list
print(f"List lookup time estimated:\t{(time.time()-t0)*10}s, mem size {sys.getsizeof(inode_list)/1024**2} MiB")


# repeat 50000 times
lol_size = 1000
inode_lol = [ [] for i in range(lol_size) ]
for inode in range(inode, inode+100000):
    inode_lol[ inode % lol_size ].append(inode)
t0 = time.time()
for i in range(50000):
    inode = random.randint(inode, inode+100000)
    inode in inode_lol[ inode % lol_size ]
print(f"List of lists lookup time:\t{(time.time()-t0)}s, mem size {(sys.getsizeof(inode_lol) + sum([sys.getsizeof(element) for element in inode_lol]) ) /1024**2} MiB")






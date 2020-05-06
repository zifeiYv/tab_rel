# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/12 16:03
@Author   : Jarvis
"""
import multiprocessing
from pybloom import BloomFilter


def do_calculation(value):
    data, capacity = value
    bf = BloomFilter(capacity=capacity)
    for i in data:
        bf.add(i)
    return bf


def add_operation(value, capacity):
    size = value.shape[0]
    pool_size = int(multiprocessing.cpu_count())
    batch = int(size/pool_size)
    inputs = []
    for i in range(pool_size):
        inputs.append((value.iloc[i*batch: (i+1)*batch, 0], capacity))
    pool = multiprocessing.Pool(
        processes=pool_size
    )
    pool_outputs = pool.map(do_calculation, inputs)
    pool.close()
    pool.join()
    bf = BloomFilter(capacity=capacity)
    for b in pool_outputs:
        bf = bf.union(b)
    return bf

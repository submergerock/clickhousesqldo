#!/opt/anaconda2/bin/python2.7
# -*- coding: utf8 -*-

'''
Created on 2018年10月11日

@author: wzt
设计思路:
1：this clickhouse example code
'''

from clickhouse_driver.client import Client

client = Client('172.20.10.8')

client.execute('SHOW TABLES')

client.execute('DROP TABLE IF EXISTS test')

client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')

client.execute(
    'INSERT INTO test (x) VALUES',
    [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
)
client.execute('INSERT INTO test (x) VALUES', [[200]])

print(client.execute('SELECT sum(x) FROM test'))
# -*- coding: utf-8 -*-
"""
@Project: table_relation_new
@Time   : 2019/8/7 16:15
@Author : sunjw
"""
import pymysql


def connection_checker(config):
    try:
        _ = pymysql.connect(**config)
        return False
    except Exception as e:
        return e

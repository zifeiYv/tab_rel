# -*- coding: utf-8 -*-
"""
@Time       : 2020/2/7 16:41
@Author     : Jarvis
@Annotation : Sorry for this shit code
"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Parameters in this file will be effective once the web app is reactivated.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
url = '/all_tables_relation/'
# 使用了gunicorn来启动服务，不再通过此处指定端口
# port = 5002

# If columns are treated both as primary key and foreign key. Default is `False`.
both_roles = False

# A string combined by some table names and separated by ',',
# used to declare tables that are not allowed to
# contain primary keys.
# Default is `None`.
not_base_table = None

# A string combined by some table names and separated by ',',
# used to declare tables that are not allowed to
# contain foreign keys.
# Default is `None`.
not_cite_table = None

# A float range from 0 to 1. Declare the upper bound of percentage
# of the number of foreign key columns not included in
# the primary key column to the total number of foreign key values.
# Default is `0`, which means a strict citation relationship.
sup_out_foreign_key = 0

# Whether use multiprocess to speed calculation, default is not, set it to `1` to turn on.
# Fast mode may take too much CPU and slow down other program.
# Multiple processes will be started only when the number of rows is larger than 1e6,
# even with mode on
multi_process = 1

# Redis config, used to show progress.
redis_config = {'host': 'localhost', 'port': 6379, }

# Data type list, which contains fields could be extracted
mysql_type_list = ['VARCHAR', 'DECIMAL', 'CHAR', 'TEXT', 'INT']  # mysql and gbase are the same
oracle_type_list = ['VARCHAR2', 'CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR2']
pg_type_list = ['CHARACTER', 'TEXT']

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
port = 5002

# If columns are treated both as primary key and foreign key. Default is `False`.
both_roles = False

# A string combined by some table names and separated by ',', used to declare tables that are not allowed to
# contain primary keys.
# Default is `None`.
not_base_table = None

# A string combined by some table names and separated by ',', used to declare tables that are not allowed to
# contain foreign keys.
# Default is `None`.
not_cite_table = None

# A float range from 0 to 1. Declare the upper bound of percentage of the number of foreign key columns not included in
# the primary key column to the total number of foreign key values.
# Default is `0`, which means a strict citation relationship.
sup_out_foreign_key = 0

# Whether use multiprocess to speed calculation, default is not,
# set it to `1` to turn on.
# Fast mode may take too much CPU and slow down other program.
multi_process = 0

# Redis config, used to show progress.
redis_config = {'host': 'localhost', 'port': 6379, }

# Request url when calculation finished.
# finish_url = 'http://127.0.0.1:5002/finish_rela_calculation/'
finish_url = 'http://127.0.0.1:5002/all_tables_relation/get_progress/'

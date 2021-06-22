# -*- coding: utf-8 -*-

# 一个字段是否可以既作为主键，又作为外键
both_roles = True

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

# 是否使用多进程加速计算，0（默认）表示关闭；1表示开启并使用os.cpu_count()个进程；
# 如果想使用指定数量的进程，直接输入对应的数字。
multi_process = 0

# Data type list, which contains fields could be extracted
mysql_type_list = ['VARCHAR', 'DECIMAL', 'CHAR', 'TEXT', 'INT']  # mysql and gbase are the same
oracle_type_list = ['VARCHAR2', 'CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR2']
pg_type_list = ['CHARACTER', 'TEXT']

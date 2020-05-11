# -*- coding: utf-8 -*-
"""
@Project: table_relation_new
@Time   : 2019/8/7 15:28
@Author : sunjw
"""
import pymysql
# import MySQLdb as pymysql
from ConnectionChecker import connection_checker
import cx_Oracle
import json
from configparser import ConfigParser
# from logger import get_logger
import datetime
from pybloom import BloomFilter
import pickle
import os
import time
import pandas as pd
import uuid
import psycopg2
import shutil

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

sys_config = ConfigParser()
with open('./sys_config.config', encoding='utf-8') as ff:
    sys_config.read_file(ff)

supOutForeignKey = sys_config.getfloat('all_paras', 'supOutForeignKey')
notBaseTable = sys_config.get('all_paras', 'notBaseTable')
notCiteTable = sys_config.get('all_paras', 'notCiteTable')
bothRoles = sys_config.getboolean('all_paras', 'bothRoles')

notBaseTableList = [] if notBaseTable == "None" else list(notBaseTable.split(','))
notCiteTableList = [] if notCiteTable == "None" else list(notCiteTable.split(','))


def func(col):
    """仅供field_name_filter函数使用"""
    if not data_cleansing.keys():
        return None
    for v in data_cleansing['_']:
        if v.endswith("%"):
            if col == v[:-1]:
                return None
            else:
                return col
        else:
            if col.startswith(v):
                return None
            else:
                return col


def field_name_filter(table, col):
    """根据data_cleansing判断table的col字段是否予以剔除"""
    if not data_cleansing:
        return col
    if table not in data_cleansing.keys():
        return func(col)
    else:
        if func(col):
            for v in data_cleansing[table]:
                if v.endswith("%"):
                    if col == v[:-1]:
                        return None
                    else:
                        continue
                else:
                    if col.startswith(v):
                        return None
                    else:
                        continue
            return col
        else:
            return None


def field_value_filter(df):
    """根据值得情况判断是否该字段可以用于计算关联关系，用于对可能的外键字段的值的判断"""
    if eval(use_str_len):
        aveg_len = df.iloc[:, 0].astype('str').str.len().mean()
    else:
        aveg_len = 999
    if aveg_len < inf_str_len:  # 平均长度小于长度下界，则不予计算
        return None
    else:
        null_num = len(df[df.iloc[:, 0].isin([None, ''])])
        if sum(-df.duplicated()) < df.shape[0] * inf_dup_ratio:
            return None
        elif null_num/len(df) > 0.8:
            return None
        else:
            return df


def compute_pk_and_cols(cr, tabs, logging, dtype_list, sql2, sql3, sql4, sql7, sql8):
    """根据tabs中的表名来计算其候选主键与候选外键"""
    cols = {}
    length = {}
    length_long = {}
    length_zero = []
    no_pks = []
    pks = {}
    no_exist = []
    for i in range(len(tabs)):
        start_time = time.clock()
        tab = tabs[i]
        logging.info(f'{i+1}/{len(tabs)}:开始识别表{tab}的主外键')
        try:
            cr.execute(sql7 % tab)
        except Exception as e:
            no_exist.append(tab)
            logging.info(f'表 {tab}在数据库中不存在:{e}！')
            continue
        row_num = cr.fetchone()[0]
        if row_num > 1e8:
            logging.info(f'表 {tab}超过一亿行！')
            length_long[tab] = row_num
            continue
        if row_num == 0:
            logging.info(f'表 {tab}中没有数据！')
            length_zero.append(tab)
            continue
        length[tab] = row_num
        check_col_sql = sql2 % tab
        try:
            cr.execute(check_col_sql)
        except Exception as e:
            logging.error(e)
            continue
        cols_datatype = cr.fetchall()
        possi_cols = []
        possi_pks = []
        for j in range(len(cols_datatype)):
            col_name = cols_datatype[j][0]
            col_type = cols_datatype[j][1]
            if col_type.upper() not in dtype_list:
                continue
            if not field_name_filter(tab, col_name):
                continue
            try:
                cr.execute(sql3 % (col_name, tab))
            except Exception as e:
                logging.error(e)
                logging.error(sql3 % (col_name, tab))
                continue
            num1 = cr.fetchone()[0]
            if num1 == row_num:
                try:
                    cr.execute(sql4 % (col_name, tab))
                except Exception as e:
                    logging.error(e)
                    logging.error(sql4 % (col_name, tab))
                    continue
                num2 = cr.fetchone()[0]
                try:
                    cr.execute(sql8 % (tab, col_name, col_name))
                except Exception as e:
                    logging.error(e)
                    logging.error(sql8 % (tab, col_name, col_name))
                    continue
                num3 = cr.fetchone()[0]
                if num2 == num1 and num1 == num3:
                    possi_pks.append(col_name)
                    if bothRoles:
                        possi_cols.append(col_name)
                elif num3 == num1:
                    possi_cols.append(col_name)
            else:
                possi_cols.append(col_name)
        if len(possi_pks):
            pks[tab] = possi_pks
            logging.info(f'{" " * 6} 表 {tab}找到可能的主键数量:{len(possi_pks)}')
        else:
            no_pks.append(tab)
            logging.info(f'{" " * 6} 表 {tab}未识别到可能的主键')
        if len(possi_cols):
            cols[tab] = possi_cols
            logging.info(f'{" " * 6} 表 {tab}找到可能有外键关系的字段数量:{len(possi_cols)}')
        else:
            cols[tab] = []
            logging.info(f'{" " * 6} 表 {tab}没有合适的字段可用于计算')
        run_times = time.clock()-start_time
        logging.info(f'{tab}统计信息 || 表记录数:{row_num} \t 字段数量:{len(cols_datatype)} \t 识别主键数量:{len(possi_pks)} \t 识别外键数量:{len(possi_cols)} \t 运行耗时(秒):%.3f' % run_times)
    logging.info("----------------------------------------------------------------------------")
    count_cols = 0
    for _, c in cols.items():
        count_cols += len(c)
    count_pks = 0
    for _, pk in pks.items():
        count_pks += len(pk)
    count_length = 0
    for _, le in length.items():
        count_length += le
    count_len_zero = len(length_zero)
    logging.info(f'总体统计信息 || 数据库表数:{len(tabs)} \t 所有表总行数:{count_length} \t 空表数量:{count_len_zero} \t 识别总主键数量:{count_pks} \t 识别总外键数量:{count_cols} \t ')
    logging.info("----------------------------------------------------------------------------")
    return cols, length, length_long, length_zero, no_pks, pks, no_exist


def generate_bloom_filter(pks, length, path, conn, logging, sql5):
    """生成过滤器"""
    if not os.path.exists(f'./filters/{path}'):
        os.makedirs(f'./filters/{path}')

    logging.info(f'尝试获取已有的过滤器文件...')
    try:
        with open(f'./filters/{path}/filters.json') as f:
            filters = json.load(f)
        logging.info(f'获取成功！')
    except Exception as e:
        filters = {}
        logging.info(f'获取失败！{e}')

    total_num = sum(list(map(lambda x: len(pks[x]), list(pks))))
    logging.info(f'最多需要创建{total_num}个过滤器！')
    n = 1
    for i in range(len(pks)):
        tab = list(pks.keys())[i]
        capacity = length[tab] * 2
        cols = pks[tab]
        for k in range(len(cols)):
            t_s = time.time()
            col = cols[k]
            logging.info(f'{n:4}/{total_num:4}:正在计算{tab}.{col}，共{length[tab]}行！')
            if tab + '@' + col in filters:
                logging.info(' ' * 10 + f'{tab}.{col}已经存在，计算下一个')
                n += 1
                continue
            value = pd.read_sql(sql5 % (col, tab), conn, coerce_float=False)
            bf = BloomFilter(capacity)
            for j in value.iloc[:, 0]:
                bf.add(j)
            with open(f'./filters/{path}/{tab}@{col}.filter', 'wb') as f:
                pickle.dump(bf, f)
            filters[tab + '@' + col] = capacity
            t_e = time.time()
            logging.info(' ' * 10 + f'{tab}.{col}已完成，耗时{t_e - t_s:.2f}秒')
            n += 1
    with open(f'./filters/{path}/filters.json', 'w') as f:
        json.dump(filters, f)


def insert_into_db(output, config_map, lastRelation, logging):
    """将计算的结果写入结果库"""
    conn = pymysql.connect(**config_map, charset='utf8')
    cr = conn.cursor()
    new_relation = 0
    for i in range(output.shape[0]):
        _id = str(uuid.uuid1()).replace("-", "")
        line = output.iloc[i, :]
        model1 = line['model1']
        db1 = line['db1']
        table1 = line['table1']
        table1comment = line['table1comment'].replace("\'", "\\'").replace("\"", "\\")
        column1 = line['column1']
        column1comment = line['column1comment'].replace("\'", "\\'").replace("\"", "\\")
        model2 = line['model2']
        db2 = line['db2']
        table2 = line['table2']
        table2comment = line['table2comment'].replace("\'", "\\'").replace("\"", "\\")
        column2 = line['column2']
        column2comment = line['column2comment'].replace("\'", "\\'").replace("\"", "\\")
        matching_degree = line['matching_degree']
        if (db1 + table1 + column1 + db2 + table2 + column2) in lastRelation:
            status = 0
        else:
            status = 1
            new_relation += 1
        model_id = model2 + model1 if model1 > model2 else model1 + model2
        insert_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        sql = f'insert into pf_analysis_result1(`id`, `model`, `model1`, `db1`, `table1`, `table1_comment`, `column1`, `column1_comment`, `model2`, `db2`, `table2`, `table2_comment`, ' \
            f'`column2`, `column2_comment`, `status`,`scantype`,`create_time`,`edit_time`, `matching_degree`) values("{_id}", "{model_id}", "{model1}", "{db1}", "{table1}", "{table1comment}","{column1}", "{column1comment}",' \
            f'"{model2}", "{db2}", "{table2}", "{table2comment}","{column2}", "{column2comment}","{status}","0","{insert_time}","{insert_time}", "{matching_degree}")'
        try:
            cr.execute(sql)
        except Exception as e:
            logging.error(e)
            continue
    try:
        conn.commit()
        cr.close()
        conn.close()
    except Exception as e:
        logging.error(e)
    return new_relation


def connect(data_source):
    """根据data_source中的内容，连接到对应的数据源"""
    if data_source['db_type'].upper() in ('MYSQL', 'GBASE'):
        db_info = data_source['config']
        if "pattern" in db_info.keys():
            db_info.pop("pattern")
        conn = pymysql.connect(**db_info, charset='utf8')
        cr = conn.cursor()
        dtype_list = ['VARCHAR', 'DECIMAL', 'CHAR', 'TEXT']
        db = data_source['config']['db']
        sql1 = f'select `table_name` from information_schema.tables where table_schema=\'{db}\' and TABLE_TYPE = \'BASE TABLE\' '
        sql2 = f'select column_name, data_type from information_schema.columns ' \
            f'where table_schema=\'{db}\' and table_name=\'%s\''
        sql3 = f'select count(`%s`) from {db}.`%s`'
        # sql4 = f'select count(distinct `%s`) from {db}.%s'
        sql4 = f'select count(1) from (select distinct `%s` from {db}.`%s`) as new_tab'
        sql5 = f'select `%s` from {db}.`%s`'
        sql6 = f'select `%s` from {db}.`%s` limit 1000'
        sql7 = f'select count(1) from {db}.`%s`'
        sql8 = f'SELECT COUNT(*) FROM {db}.`%s` WHERE length(`%s`)=char_length(`%s`) '
    elif data_source['db_type'].upper() == 'ORACLE':
        url = data_source['config']['host'] + ':' + str(data_source['config']['port']) + '/' + data_source['config']['db']
        conn = cx_Oracle.connect(data_source['config']['user'], data_source['config']['password'], url)
        cr = conn.cursor()
        dtype_list = ['VARCHAR2', 'NUMBER']
        db = data_source['config']['user']
        sql1 = 'select table_name from user_tables'
        sql2 = f'select u.column_name, u.data_type from user_tab_columns u left join all_tables a ' \
            f'on u.table_name = a.table_name where u.table_name=\'%s\''
        sql3 = f'select count(%s) from {db}.%s'
        sql4 = f'select count(distinct %s) from {db}.%s'
        sql5 = f'select %s from {db}.%s'
        sql6 = f'select %s from {db}.%s where rownum <= 1000'
        sql7 = f'select count(1) from {db}.%s'
        sql8 = f'SELECT count(1) FROM {db}."%s" WHERE LENGTH("%s") = LENGTHB("%s")'
    elif data_source['db_type'].upper() == 'POSTGRESQL':
        data_config = data_source['config']
        dtype_list = ['CHARACTER', 'TEXT']
        conn = psycopg2.connect(host=data_config['host'], port=data_config['port'],
                         user=data_config['user'], password=data_config['password'],
                         database=data_config['db'])
        cr = conn.cursor()
        pattern = data_config['pattern']
        sql1 = f"select tablename from pg_tables where schemaname='{pattern}'"
        sql2 = f"SELECT A.ATTNAME AS NAME,SUBSTRING(format_type(a.atttypid,a.atttypmod) from '[a-zA-Z]*') AS TYPE  " \
            f"FROM PG_CLASS AS C, PG_ATTRIBUTE AS A,pg_namespace as p WHERE c.relnamespace= p.oid " \
            f"AND C.RELNAME = '%s' AND A.ATTRELID = C.OID AND A.ATTNUM > 0 and p.nspname='{pattern}'"
        sql3 = f'select count("%s") from {pattern}.%s'
        sql4 = f'select count(distinct "%s") from {pattern}.%s'
        sql5 = f'select "%s" from {pattern}.%s'
        sql6 = f'select "%s" from {pattern}.%s '
        sql7 = f'select count(1) from {pattern}.%s'
        sql8 = f'select count(*) from {pattern}.%s where length("%s") = octet_length("%s")'
    else:
        conn = cr = dtype_list = sql1 = sql2 = sql3 = sql4 = sql5 = sql6 = sql7 = sql8 = None
    return conn, cr, data_source['config']['db'], dtype_list, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8


def get_cached_files(path, ori_tables, logging):
    """获取缓存文件（如果存在的话）"""
    logging.info(f'尝试获取缓存文件')
    try:
        with open(f'./table_attr/{path}/cols.json') as f:
            cached_cols = json.load(f)
        with open(f'./table_attr/{path}/length.json') as f:
            cached_length = json.load(f)
        with open(f'./table_attr/{path}/length_long.json') as f:
            cached_length_long = json.load(f)
        with open(f'./table_attr/{path}/length_zero.json') as f:
            cached_length_zero = json.load(f)
        with open(f'./table_attr/{path}/no_pks.json') as f:
            cached_no_pks = json.load(f)
        with open(f'./table_attr/{path}/pks.json') as f:
            cached_pks = json.load(f)
        with open(f'./table_attr/{path}/no_exist.json') as f:
            cached_no_exist = json.load(f)

        new_tables = list(set(ori_tables) - set(cached_length) - set(cached_length_long)
                          - set(cached_length_zero) - set(cached_no_exist))
        cols = cached_cols.copy()
        length = cached_length.copy()
        length_long = cached_length_long.copy()
        length_zero = cached_length_zero.copy()
        no_pks = cached_no_pks.copy()
        pks = cached_pks.copy()
        no_exist = cached_no_exist.copy()
        logging.info(f'缓存文件获取成功！')
    except IOError:
        logging.info(f'缓存文件获取失败！')
        new_tables = ori_tables.copy()
        cols = {}
        length = {}
        length_long = {}
        length_zero = []
        no_pks = []
        pks = {}
        no_exist = []
    return new_tables, cols, length, length_long, length_zero, no_pks, pks, no_exist


def get_table_column_comment(data_source, ori_tables, cr, logging):
    source_type = data_source['db_type'].upper()
    _tables = []
    for t in ori_tables:
        _tables.append("'" + t + "'")
    _tables = ",".join(_tables)
    table_comment = {}
    column_comment = {}
    if source_type in ("MYSQL", "GBASE"):
        try:
            tab_com_sql = f"select table_name,table_comment from information_schema.`TABLES` where table_schema = '{data_source['config']['db']}' and table_name in ({_tables})"
            cr.execute(tab_com_sql)
            t_c = cr.fetchall()
            for t in t_c:
                table_comment[t[0]] = t[1] if t[1] is not None else ""
            col_com_sql = f"select table_name,column_name,column_comment from information_schema.`COLUMNS` where table_schema = '{data_source['config']['db']}' and table_name in ({_tables})"
            cr.execute(col_com_sql)
            c_c = cr.fetchall()
            for c in c_c:
                column_comment[c[0] + c[1]] = c[2] if c[2] is not None else ""
        except Exception as e:
            logging.error(e)
    if source_type == "ORACLE":
        try:
            tab_com_sql = f"select t.table_name, f.comments from user_tables t inner join user_tab_comments f on t.table_name = f.table_name where t.table_name in ({_tables})"
            cr.execute(tab_com_sql)
            t_c = cr.fetchall()
            for t in t_c:
                table_comment[t[0]] = t[1] if t[1] is not None else ""
            col_com_sql = f"SELECT t.TABLE_NAME,t.COLUMN_NAME,a.COMMENTS  FROM USER_TAB_COLUMNS t LEFT JOIN USER_COL_COMMENTS a ON t.table_name = a.table_NAME AND t.COLUMN_NAME = a.COLUMN_NAME where t.TABLE_NAME in ({_tables})"
            cr.execute(col_com_sql)
            c_c = cr.fetchall()
            for c in c_c:
                column_comment[c[0] + c[1]] = c[2] if c[2] is not None else ""
        except Exception as e:
            logging.error(e)
    return table_comment, column_comment


def one_db(data_source, logging):
    """连接到一个指定的数据源，然后对其中指定的表进行关系发现"""
    conn, cr, path, dtype_list, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8 = connect(data_source)
    # 判断表的是否指定了表，如果未指定，则自动查找数据库中所有的表进行计算
    if not data_source['tables']:
        logging.info(f'未指定表名，自动获取数据库中所有的表进行计算')
        print(sql1)
        cr.execute(sql1)
        ori_tables = list(map(lambda x: x[0], cr.fetchall()))
    else:
        logging.info(f'用户指定了表名，将按照指定表进行计算')
        ori_tables = data_source['tables']

    table_comment, column_comment = get_table_column_comment(data_source, ori_tables, cr, logging)

    new_tables, cols, length, length_long, length_zero, no_pks, pks, \
    no_exist = get_cached_files(path, ori_tables, logging)

    logging.info(f'计算主键与外键...')
    new_cols, new_length, new_length_long, new_length_zero, new_no_pks, new_pks, \
    new_no_exist = compute_pk_and_cols(cr, new_tables, logging, dtype_list, sql2, sql3, sql4, sql7, sql8)
    logging.info(f'计算完成！')

    logging.info(f'更新（创建）缓存文件...')
    cols.update(new_cols)
    length.update(new_length)
    length_long.update(new_length_long)
    length_zero += new_length_zero
    no_pks += new_no_pks
    pks.update(new_pks)
    no_exist += new_no_exist

    ori_tables = list(set(ori_tables) - set(no_exist) - set(length_zero) - set(length_long))
    if not os.path.exists(f'./table_attr/{path}'):
        os.makedirs(f'./table_attr/{path}')
    for v in ['cols', 'length', 'length_long', 'length_zero', 'no_pks', 'pks', 'no_exist']:
        with open(f'./table_attr/{path}/{v}.json', 'w') as f:
            json.dump(eval(v), f)
    logging.info(f'更新（创建）完成！')

    logging.info(f'计算过滤器...')
    generate_bloom_filter(pks, length, path, conn, logging, sql5)
    logging.info(f'计算完成！')

    logging.info(f'计算表关系...')
    results = []
    for i in range(len(ori_tables)):
        tab = ori_tables[i]
        logging.info(f'计算{tab}的关联关系，第{i+1}个（共{len(ori_tables)}个）')
        if tab in notCiteTableList:
            continue
        if length[tab] < inf_tab_len:
            continue
        for col in cols[tab]:
            try:
                value = pd.read_sql(sql6 % (col, tab), conn, coerce_float=False)
                # value = value.drop_duplicates()
                all_num = value.shape[0]
            except Exception as e:
                logging.exception(e)
                continue
            if field_value_filter(value) is None:
                continue
            for pk_name in pks:
                if pk_name in notBaseTableList:
                    continue
                if length[pk_name] < inf_tab_len:
                    continue
                if pk_name == tab:
                    continue
                for pk_col in pks[pk_name]:
                    if "".join((path, pk_name, pk_col, path, tab, col)) in UserRelation:
                        continue
                    with open(f'./filters/{path}/{pk_name}@{pk_col}.filter', 'rb') as f:
                        bf = pickle.load(f)
                    flag = 1
                    num_not_in_pk = 0.0
                    for k in value[col]:
                        if k not in bf:
                            num_not_in_pk += 1
                        if num_not_in_pk / all_num > supOutForeignKey:
                            flag = 0
                            break
                    if flag:
                        matching_degree = num_not_in_pk / all_num
                        # matching_degree
                        results.append([data_source['model_id'], path, pk_name, table_comment[pk_name] if pk_name in table_comment.keys() else '', pk_col, column_comment[pk_name+pk_col] if (pk_name+pk_col) in column_comment.keys() else '',
                                        data_source['model_id'], path, tab, table_comment[tab] if tab in table_comment.keys() else '',  col, column_comment[tab+col] if (tab+col) in column_comment.keys() else '', matching_degree])
    output = pd.DataFrame(columns=['model1', 'db1', 'table1', 'table1comment', 'column1', 'column1comment', 'model2', 'db2', 'table2', 'table2comment', 'column2', 'column2comment', 'matching_degree'],
                          index=range(len(results)))
    for i in range(len(results)):
        output.iloc[i] = results[i]
    print(output)
    return output


def data_source_save(data_source1, data_source2, logging):
    conn1, cr1, path1, dtype_list1, sql11, sql12, sql13, sql14, sql15, sql16, sql17, sql18 = connect(data_source1)
    conn2, cr2, path2, dtype_list2, sql21, sql22, sql23, sql24, sql25, sql26, sql27, sql28 = connect(data_source2)

    if not data_source1['tables']:
        logging.info(f'data-source1未指定表名，自动获取数据库中所有的表进行计算')
        cr1.execute(sql11)
        ori_tables1 = list(map(lambda x: x[0], cr1.fetchall()))
    else:
        logging.info(f'data-source1指定了表名，将据此进行计算')
        ori_tables1 = data_source1['tables']

    if not data_source2['tables']:
        logging.info(f'data-source2未指定表名，自动获取数据库中所有的表进行计算')
        cr2.execute(sql21)
        ori_tables2 = list(map(lambda x: x[0], cr2.fetchall()))
    else:
        logging.info(f'data-source2指定了表名，将据此进行计算')
        ori_tables2 = data_source2['tables']

    new_tables1, cols1, length1, length_long1, length_zero1, no_pks1, pks1, \
    no_exist1 = get_cached_files(path1, ori_tables1, logging)

    new_tables2, cols2, length2, length_long2, length_zero2, no_pks2, pks2, \
    no_exist2 = get_cached_files(path2, ori_tables2, logging)

    logging.info(f'计算data-source1的主键与外键...')
    new_cols1, new_length1, new_length_long1, new_length_zero1, new_no_pks1, new_pks1, \
    new_no_exist1 = compute_pk_and_cols(cr1, new_tables1, logging, dtype_list1, sql12, sql13, sql14, sql17, sql18)
    logging.info(f'data-source1计算完成！')

    logging.info(f'计算data-source2的主键与外键...')
    new_cols2, new_length2, new_length_long2, new_length_zero2, new_no_pks2, new_pks2, \
    new_no_exist2 = compute_pk_and_cols(cr2, new_tables2, logging, dtype_list2, sql22, sql23, sql24, sql27, sql28)
    logging.info(f'data-source2计算完成！')

    logging.info(f'更新（创建）data-source1的缓存文件...')
    cols1.update(new_cols1)
    length1.update(new_length1)
    length_long1.update(new_length_long1)
    length_zero1 += new_length_zero1
    no_pks1 += new_no_pks1
    pks1.update(new_pks1)
    no_exist1 += new_no_exist1
    # ori_tables1 = list(set(ori_tables1) - set(no_exist1) - set(length_zero1) - set(length_long1))
    if not os.path.exists(f'./table_attr/{path1}'):
        os.makedirs(f'./table_attr/{path1}')
    for v in ['cols1', 'length1', 'length_long1', 'length_zero1', 'no_pks1', 'pks1', 'no_exist1']:
        with open(f'./table_attr/{path1}/{v.replace("1", "")}.json', 'w') as f:
            json.dump(eval(v), f)
    logging.info(f'data-source1更新（创建）完成！')

    logging.info(f'更新（创建）data-source2的缓存文件...')
    cols2.update(new_cols2)
    length2.update(new_length2)
    length_long2.update(new_length_long2)
    length_zero2 += new_length_zero2
    no_pks2 += new_no_pks2
    pks2.update(new_pks2)
    no_exist2 += new_no_exist2
    if not os.path.exists(f'./table_attr/{path2}'):
        os.makedirs(f'./table_attr/{path2}')
    for v in ['cols2', 'length2', 'length_long2', 'length_zero2', 'no_pks2', 'pks2', 'no_exist2']:
        with open(f'./table_attr/{path2}/{v.replace("2", "")}.json', 'w') as f:
            json.dump(eval(v), f)
    logging.info(f'data-source2更新（创建）完成！')
    cr1.close()
    cr2.close()
    conn1.close()
    conn2.close()
    return ori_tables1, ori_tables2


def two_dbs(data_source1, data_source2, logging, ori_tables1, ori_tables2):
    """连接到两个指定的数据源，然后对其中指定的表进行关系发现
        以data_source1为主键表。
    """
    conn1, cr1, path1, dtype_list1, sql11, sql12, sql13, sql14, sql15, sql16, sql17, sql18 = connect(data_source1)
    conn2, cr2, path2, dtype_list2, sql21, sql22, sql23, sql24, sql25, sql26, sql27, sql28 = connect(data_source2)

    new_tables1, cols1, length1, length_long1, length_zero1, no_pks1, pks1, \
    no_exist1 = get_cached_files(path1, ori_tables1, logging)

    new_tables2, cols2, length2, length_long2, length_zero2, no_pks2, pks2, \
    no_exist2 = get_cached_files(path2, ori_tables2, logging)

    ori_tables2 = list(set(ori_tables2) - set(no_exist2) - set(length_zero2) - set(length_long2))
    logging.info(f'计算过滤器...')
    generate_bloom_filter(pks1, length1, path1, conn1, logging, sql15)
    logging.info(f'计算完成！')

    logging.info(f'计算表关系...')

    tab_comment1, col_comment1 = get_table_column_comment(data_source1, ori_tables1, cr1, logging)
    tab_comment2, col_comment2 = get_table_column_comment(data_source2, ori_tables2, cr2, logging)

    results = []
    for i in range(len(ori_tables2)):
        tab = ori_tables2[i]
        logging.info(f'计算{tab}的关联关系，第{i}个（共{len(ori_tables2)}个）')
        if tab in notCiteTableList:
            continue
        if length2[tab] < inf_tab_len:
            continue
        for col in cols2[tab]:
            try:
                value = pd.read_sql(sql26 % (col, tab), conn2, coerce_float=False)
                # value = value.drop_duplicates()
                all_num = value.shape[0]
            except Exception as e:
                logging.exception(e)
                continue

            if field_value_filter(value) is None:
                continue
            for pk_name in pks1:
                if pk_name in notBaseTableList:
                    continue
                if pk_name == tab:
                    continue
                for pk_col in pks1[pk_name]:
                    if "".join((path1, pk_name, pk_col, path2, tab, col)) in UserRelation:
                        continue
                    with open(f'./filters/{path1}/{pk_name}@{pk_col}.filter', 'rb') as f:
                        bf = pickle.load(f)
                    flag = 1
                    num_not_in_pk = 0
                    for k in value[col]:
                        if k not in bf:
                            num_not_in_pk += 1
                        if num_not_in_pk / all_num > supOutForeignKey:
                            flag = 0
                            break
                    if flag:
                        results.append([data_source1['model_id'], path1, pk_name, tab_comment1[pk_name] if pk_name in tab_comment1.keys() else '', pk_col, col_comment1[pk_name+pk_col] if (pk_name+pk_col) in col_comment1.keys() else '',
                                        data_source2['model_id'], path2, tab, tab_comment2[tab] if tab in tab_comment2.keys() else '', col, col_comment2[tab+col]] if (tab+col) in col_comment2.keys() else '')
    output = pd.DataFrame(columns=['model1', 'db1', 'table1', 'table1comment', 'column1', 'column1comment', 'model2', 'db2', 'table2', 'table2comment', 'column2', 'column2comment'],
                          index=range(len(results)))
    for i in range(len(results)):
        output.iloc[i] = results[i]
    print(output)
    return output


def run_process(post_json):
    tab_rela_strat_time = datetime.datetime.now()
    from logger import get_logger
    config_map = post_json['configMap']

    # 判断配置数据库是否可以连接成功
    res = connection_checker(config_map)
    if res:
        return {'state': 0, 'msg': f'无法连接配置数据库：{res}'}

    # 连接配置库，获取计算用到的参数
    connection = pymysql.connect(**config_map, charset='utf8')
    model1 = post_json['modelId1']
    model2 = post_json['modelId2']
    algorithm_name = post_json['algorithmName']
    execut_obj = post_json['executObj']
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    model_id = (model2 + model1) if model1 > model2 else (model1 + model2)
    # 获取模型参数配置信息
    logging, handler = get_logger(model_id)
    logging.info("算法开始运行所对应的模型包括：" + model1 + model2)
    # 初始化状态表，1表示算法运行，2表示算法运行完成
    state_sql = f"REPLACE INTO pf_analysis_status (linkageid,analysisstatus,algorithmname,executobj,start_time)" \
        f" VALUES('{model_id}', '1','{algorithm_name}','{execut_obj}','{start_time}')"
    print(state_sql)
    with connection.cursor() as cr:
        cr.execute(state_sql)
        connection.commit()

    sql = f'select t.CSL, t.DS, t.iDR, t.iSL, t.iTL, t.tables1, t.tables2 ' \
        f'from pf_user_config t where t.model="{model_id}"'
    global use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len
    with connection.cursor() as cr:
        if cr.execute(sql):  # 如果在配置表中查询到用户自定义的参数，则获取该参数
            logging.info(f"成功获取到{model_id}自定义的参数！")
            res = cr.fetchone()
            use_str_len = str(res[0]) if res[0] else '0'
            data_cleansing = eval(str(res[1])) if res[1] else {'_': ['EXT_', 'ext_']}
            inf_dup_ratio = res[2] if res[2] else 0.4
            inf_str_len = res[3] if res[3] else 3
            inf_tab_len = res[4] if res[4] else 10
            tables1 = list(res[5].split(',')) if res[5] else []
            tables2 = list(res[6].split(',')) if res[6] else []
        else:  # 否则，将这些参数设置为默认值
            logging.info(f"未获取到{model_id}的自定义参数，将采用默认参数计算！")
            use_str_len = "0"
            data_cleansing = {'_': ['EXT_', 'ext_']}
            inf_dup_ratio = 0.4
            inf_str_len = 3
            inf_tab_len = 10
            tables1 = []
            tables2 = []

    # 删除之前计算得到的结果（如果有的话），注意凡是经过用户修改过的，均予以保留
    global UserRelation
    global lastRelation
    with connection.cursor() as cr:
        sql = f'select db1, table1, column1, db2, table2, column2 from pf_analysis_result1 where model="{model_id}"'
        cr.execute(sql)
        re = cr.fetchall()
        lastRelation = []
        for r in re:
            lastRelation.append("".join(r))
        sql = f'delete from pf_analysis_result1  where  model="{model_id}" and  `SCANTYPE`="0"'
        cr.execute(sql)
        sql = f'select db1, table1, column1, db2, table2, column2 from pf_analysis_result1 where ' \
            f'model="{model_id}" and `SCANTYPE` != "0"'
        cr.execute(sql)
        res = cr.fetchall()
        UserRelation = []
        for i in range(len(res)):
            UserRelation.append("".join(res[i]))
    connection.commit()
    # 根据模型的id获取对应的数据源连接信息
    if model1 == model2:
        logging.info(f'传入的模型id相同，为同库查询。')
        sql = f'select datasource_id from pf_businessmodel_model t where t.id="{model1}"'
        with connection.cursor() as cr:
            cr.execute(sql)
            ds1 = cr.fetchone()[0]
        sql = f'select type, host, port, user_name, password, sid,schema_name from pf_datasource t where t.id="{ds1}"'
        data_source = {}
        with connection.cursor() as cr:
            cr.execute(sql)
            res = cr.fetchone()
            data_source['model_id'] = model1
            data_source['db_type'] = res[0]
            data_source['tables'] = tables1
            data_source['config'] = {
                'host': res[1],
                'port': int(res[2]),
                'user': res[3],
                'password': res[4],
                'db': res[5],
                'pattern': res[6]
            }
        output = one_db(data_source, logging)
        new_relation_num = insert_into_db(output, config_map, lastRelation, logging)
        relation_num = len(output)
    else:
        logging.info(f'传入的模型id不同，为跨库查询。')
        model_1 = model1 if model1 < model2 else model2
        model_2 = model2 if model1 < model2 else model1
        sql1 = f'select datasource_id from pf_businessmodel_model t where t.id="{model_1}"'
        sql2 = f'select datasource_id from pf_businessmodel_model t where t.id="{model_2}"'
        with connection.cursor() as cr:
            cr.execute(sql1)
            ds1 = cr.fetchone()[0]
            cr.execute(sql2)
            ds2 = cr.fetchone()[0]
        sql1 = f'select type, host, port, user_name, password, sid, schema_name from pf_datasource t where t.id="{ds1}"'
        sql2 = f'select type, host, port, user_name, password, sid, schema_name from pf_datasource t where t.id="{ds2}"'
        data_source1 = {}
        data_source2 = {}
        with connection.cursor() as cr:
            cr.execute(sql1)
            res = cr.fetchone()
            data_source1['model_id'] = model_1
            data_source1['db_type'] = res[0]
            data_source1['tables'] = tables1
            data_source1['config'] = {
                'host': res[1],
                'port': int(res[2]),
                'user': res[3],
                'password': res[4],
                'db': res[5],
                'pattern': res[6]
            }
            cr.execute(sql2)
            res = cr.fetchone()
            data_source2['model_id'] = model_2
            data_source2['db_type'] = res[0]
            data_source2['tables'] = tables2
            data_source2['config'] = {
                'host': res[1],
                'port': res[2],
                'user': res[3],
                'password': res[4],
                'db': res[5],
                'pattern': res[6]
            }
        # 数据资源信息写入文件中,data_source1与data_source2不分先后顺序
        ori_tables1, ori_tables2 = data_source_save(data_source1, data_source2, logging)
        output1 = two_dbs(data_source1, data_source2, logging, ori_tables1, ori_tables2)
        output2 = two_dbs(data_source2, data_source1, logging, ori_tables2, ori_tables1)
        new_relation_num1 = insert_into_db(output1, config_map, lastRelation, logging)
        new_relation_num2 = insert_into_db(output2, config_map, lastRelation, logging)
        new_relation_num = new_relation_num1+new_relation_num2
        relation_num = len(output1) + len(output2)
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 更新初始化状态表，1表示算法运行，2表示算法运行完成
    state_sql = f"update pf_analysis_status set analysisstatus = '2',relationnum={relation_num},new_relation_num={new_relation_num},end_time='{end_time}'" \
        f" where linkageid = '{model_id}'"
    with connection.cursor() as cr:
        try:
            cr.execute(state_sql)
            connection.commit()
        except Exception as e:
            logging.error(e)
    connection.close()
    tab_rela_end_time = datetime.datetime.now()
    logging.info("算法运行完成 || 起始时间："+tab_rela_strat_time.strftime('%Y-%m-%d  %H:%M:%S.%f') + " ||  结束时间:" + tab_rela_end_time.strftime("%Y-%m-%d %H:%M:%S.%f") + "  ||  算法总耗时：" + use_time((tab_rela_end_time-tab_rela_strat_time).seconds))
    logging.removeHandler(handler)


def model_run_info_census(path, logging):
    logging.info(f"统计数据源 {path} 的基本情况......")
    try:
        with open(f'./table_attr/{path}/cols.json') as f:
            cached_cols = json.load(f)
        with open(f'./table_attr/{path}/length.json') as f:
            cached_length = json.load(f)
        with open(f'./table_attr/{path}/length_long.json') as f:
            cached_length_long = json.load(f)
        with open(f'./table_attr/{path}/length_zero.json') as f:
            cached_length_zero = json.load(f)
        with open(f'./table_attr/{path}/no_pks.json') as f:
            cached_no_pks = json.load(f)
        with open(f'./table_attr/{path}/pks.json') as f:
            cached_pks = json.load(f)
        with open(f'./table_attr/{path}/no_exist.json') as f:
            cached_no_exist = json.load(f)
    except Exception as e:
        logging.error(e)

    pass


def use_time(seconds):
    if seconds < 60:
        return str(seconds) + " 秒"
    elif 60 <= seconds < 60*60:
        min = int(seconds/60)
        sec = seconds - (min * 60)
        return str(min) + " 分 " + str(sec)+" 秒"
    elif 60*60 <= seconds:
        hou = int(seconds/(60*60))
        min = int((seconds - hou*60*60)/60)
        sec = seconds - hou*60*60 - min*60
        return str(hou) + " 时 "+str(min) + " 分 " + str(sec) + " 秒"


def del_cache_file(post_json):
    db_name = post_json['db']
    filter_path = f"./filters/{db_name}"
    db_info_path = f"./table_attr/{db_name}"
    try:
        if os.path.isdir(filter_path):
            shutil.rmtree(filter_path)
        if os.path.isdir(db_info_path):
            shutil.rmtree(db_info_path)
    except Exception as e:
        logging.error(e)
        return "0"
    return "1"

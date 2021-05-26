# -*- coding: utf-8 -*-
import os
import logging
import pymysql
from config import multi_process, mysql_type_list, both_roles, sup_out_foreign_key
import pandas as pd
from pybloom import BloomFilter
import pickle
from utils.utils import EmptyLogger
import multiprocessing


def run(model_id, tar_tables=None, custom_para=None, **db_kw):
    logger = logging.getLogger(f'{model_id}')
    if not multi_process:
        logger.info('使用单进程')
        processes = 1
    elif multi_process == 1:
        processes = os.cpu_count()
        logger.info(f'使用多进程，进程数量为：{processes}')
    else:
        processes = multi_process
        logger.info(f'使用多进程，进程数量为：{processes}')

    conn = pymysql.connect(**db_kw)
    db = db_kw['db']
    if not tar_tables:
        logger.info('用户未指定表，将读取目标库中的全表进行计算')
        with conn.cursor() as cr:
            sql = f"select table_name from information_schema.tables where table_schema='{db}' " \
                  f"and table_type='BASE TABLE'"
            cr.execute(sql)
            tables = [i[0] for i in cr.fetchall()]
        conn.close()
    else:
        logger.info('用户指定了表，将在指定表中寻找关联关系')
        tables = tar_tables
    if not tables:
        logger.warning('未发现可用表')
        return
    logger.info(f'获取表成功，共：{len(tables)}个')

    if multi_process:
        if len(tables) < 150:
            logger.warning(f'表数较少（{len(tables)}），启用多进程可能效果不佳')
    df = execute(model_id, processes, tables, **db_kw)
    return df


def execute(model_id, processes, tables, **kwargs):
    """执行函数。

    Args:
        model_id(str): 模型的唯一标识
        processes: 进程数量
        tables: 所有待计算的表名
        **kwargs:

    Returns:

    """
    logger = logging.getLogger(f'{model_id}')

    host, port, user = kwargs['host'], int(kwargs['port']), kwargs['user']
    passwd, db = kwargs['passwd'], kwargs['db']
    if processes == 1:  # 单进程
        rel_cols, pks = pre_processing(model_id, tables, False, host,
                                       port, user, passwd, db)
        output = find_rel(rel_cols, pks, model_id, False, host,
                          port, user, passwd, db)
    else:
        logger.info('多进程预处理数据...')
        if not len(tables) % processes:
            batch_size = int(len(tables) / processes)
        else:
            batch_size = int(len(tables) / processes) + 1
        q = multiprocessing.Queue()
        jobs = []
        for i in range(processes):
            if i == processes - 1:
                p = multiprocessing.Process(target=pre_processing,
                                            args=(model_id, tables[i * batch_size:],
                                                  True, host, port, user, passwd, db, q,))
            else:
                p = multiprocessing.Process(target=pre_processing,
                                            args=(model_id, tables[i * batch_size: (i + 1) * batch_size],
                                                  True, host, port, user, passwd, db, q,))
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
            p.close()
        rel_cols, pks = {}, {}
        for _ in jobs:
            _a, _b = q.get()
            rel_cols.update(_a)
            pks.update(_b)
        logger.info('多进程数据预处理完成')

        logger.info('多进程关系发现...')
        q = multiprocessing.Queue()
        jobs = []
        rel_cols_items = list(rel_cols.items())
        for i in range(processes):
            if i == processes - 1:
                p = multiprocessing.Process(target=find_rel,
                                            args=(rel_cols_items[i * batch_size:], pks,
                                                  model_id, True, host, port, user, passwd, db, q,))
            else:
                p = multiprocessing.Process(target=find_rel,
                                            args=(rel_cols_items[i * batch_size: (i + 1) * batch_size], pks,
                                                  model_id, True, host, port, user, passwd, db, q,))
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
            p.close()
        output = []
        for _ in jobs:
            res = q.get()
            output.extend(res)

    columns = ['model1', 'db1', 'table1', 'table1comment', 'column1',
               'column1comment', 'model2', 'db2', 'table2', 'table2comment',
               'column2', 'column2comment', 'matching_degree']
    if output:
        df = pd.DataFrame(columns=columns, data=output)
        logger.info('表关系查找完成')
        return df
    else:
        logger.info('未找到关系')


def pre_processing(model_id, table, multi, host, port, user, passwd, db, q=None):
    """获取table的主键和可能的外键，并针对主键生成filter文件。

    Args:
        model_id(str): 模型的唯一标识
        table: 表名，要么为list，要么为str
        multi(bool): 是否采用多进程进行计算
        host(str):
        port(int):
        user(str):
        passwd(str):
        db(str):
        q: 队列

    Returns:
        rel_cols: 一个字典，键为表名，值为该张表可能作为外键的字段列表
        pks: 一个字典，键为表名，值为该张表可能作为主键的字段列表

    """
    if not multi:
        logger = logging.getLogger(f'{model_id}')
    else:
        logger = EmptyLogger()
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)

    if isinstance(table, list):
        tables = table
    else:  # str
        tables = [table]
    sql1 = f'select count(1) from `{db}`.`%s`'
    sql2 = f'select column_name, data_type from information_schema.columns where ' \
           f"table_schema='{db}' and table_name='%s'"
    sql3 = f'select count(`%s`) from `{db}`.`%s`'
    sql4 = f'select count(distinct `%s`) from `{db}`.`%s`'
    sql5 = f'select count(1) from `{db}`.`%s` where length(`%s`)=char_length(`%s`)'
    cr = conn.cursor()
    rel_cols = {}  # 存储表及其可能与其他表主键进行关联的字段
    length_normal = {}  # 存储表及其长度
    length_too_long = {}  # 存储表及其长度；行数太长，可能导致内存溢出无法计算
    length_zero = []  # 空表
    without_pks = []  # 没有主键的表
    pks = {}  # 存储表及其可能的主键列表
    no_exist = []  # 数据库中不存在的表
    logger.info('预处理所有表')
    for tab in tables:
        logger.debug(f'  {tab}：长度校验')
        try:
            cr.execute(sql1 % tab)
            row_num = cr.fetchone()[0]
            if row_num > 1e8:
                length_too_long[tab] = row_num
                logger.debug(f'  {tab}：超长，被过滤')
                continue
            elif row_num == 0:
                length_zero.append(tab)
                logger.debug(f'  {tab}：为空，被过滤')
                continue
            else:
                length_normal[tab] = row_num
                logger.debug(f'  {tab}：长度合格')
        except Exception as e:
            logger.debug(f'  {tab}：不存在：{e}')
            no_exist.append(tab)
            continue

        logger.debug(f'  {tab}：查找主键')
        cr.execute(sql2 % tab)
        field_and_type = cr.fetchall()
        psb_pk, psb_col = [], []
        for i in range(len(field_and_type)):
            field_name = field_and_type[i][0]
            field_type = field_and_type[i][1]
            if field_type.upper() not in mysql_type_list:
                logger.debug(f'    {field_name}的数据类型是{field_type}，不属于要计算的数据类型{mysql_type_list}')
                continue
            cr.execute(sql3 % (field_name, tab))
            num1 = cr.fetchone()[0]
            if num1 == row_num:
                cr.execute(sql4 % (field_name, tab))
                num2 = cr.fetchone()[0]
                cr.execute(sql5 % (tab, field_name, field_name))
                num3 = cr.fetchone()[0]
                if num1 == num2 and num2 == num3:
                    psb_pk.append(field_name)
                    if both_roles:
                        psb_col.append(field_name)
                elif num1 == num3:
                    psb_col.append(field_name)
        rel_cols[tab] = psb_col
        if len(psb_pk):
            logger.debug(f'  {tab}：主键已保存')
            pks[tab] = psb_pk
        else:
            logger.debug(f'  {tab}：未发现主键')
            without_pks.append(tab)
            continue

        logger.debug(f'  {tab}：正在生成filter文件')
        if not os.path.exists(f'./filters/{model_id}/{db}'):
            os.makedirs(f'./filters/{model_id}/{db}')
        capacity = int(length_normal[tab] * 1.2)
        for pk in pks[tab]:
            filter_name = tab + '@' + pk + '.filter'
            if os.path.exists(f'./filters/{model_id}/{db}/{filter_name}'):
                logger.debug(f'    {tab}.{pk} 已经存在')
                continue
            value = pd.read_sql(f"select {pk} from {tab}", conn)
            bf = BloomFilter(capacity)
            for j in value.iloc[:, 0]:
                bf.add(j)
            with open(f'./filters/{model_id}/{db}/{filter_name}', 'wb') as f:
                pickle.dump(bf, f)
        logger.debug(f'  {tab}：全部filter已保存')
    logger.info('完成')
    if q:
        q.put((rel_cols, pks))
    cr.close()
    conn.close()
    return rel_cols, pks


def find_rel(rel_cols, pks, model_id, multi, host, port, user, passwd, db, q=None):
    """查找关系。

    Args:
        rel_cols(dict): 一个字典，键为表名，值为该张表可能作为外键的字段列表
        pks(dict):
        host(str):
        port(int):
        user(str):
        passwd(str):
        model_id(str):
        db(str):
        multi(bool):
        q:

    Returns:

    """
    if not multi:
        logger = logging.getLogger(model_id)
    else:
        logger = EmptyLogger()
    rel_cols_dict = {}
    if isinstance(rel_cols, list):
        for i in rel_cols:
            rel_cols_dict[i[0]] = i[1]
    else:
        rel_cols_dict = rel_cols

    sql = f'select `%s` from `{db}`.`%s` limit 10000'
    results = []
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    logger.info('计算所有关系')
    for tab in rel_cols_dict:
        for col in rel_cols_dict[tab]:
            value = pd.read_sql(sql % (col, tab), conn)
            for pk_tab in pks:
                if pk_tab == tab:
                    continue
                for pk in pks[pk_tab]:
                    with open(f'./filters/{model_id}/{db}/{pk_tab}@{pk}.filter', 'rb') as f:
                        bf = pickle.load(f)
                    flag = 1
                    num_not_in_bf = 0
                    for k in value[col]:
                        if k not in bf:
                            num_not_in_bf += 1
                        if num_not_in_bf / 10000 > sup_out_foreign_key:
                            flag = 0
                            break
                    if flag:
                        not_match_ratio = num_not_in_bf / 10000
                        res = [model_id, db, pk_tab, 'table1comment', pk, 'column1comment',
                               model_id, db,
                               tab, 'table2comment', col, 'column2comment', not_match_ratio]
                        results.append(res)
    if q:
        q.put(results)
    conn.close()
    logger.info('完成')
    return results

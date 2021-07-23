# -*- coding: utf-8 -*-
import os
import logging
from impala.dbapi import connect
from config import multi_process, both_roles, sup_out_foreign_key
import pandas as pd
from pybloom import BloomFilter
import pickle
from utils.utils import sub_process_logger
import multiprocessing
import traceback
import json
from .utils import col_name_filter, col_value_filter


def run(model_id, tar_tables=None, custom_para=None, **db_kw):
    """根据指定的Hive数据源进行关系发现的程序的主入口。

    Args:
        model_id(str): 当前融合任务的唯一标识
        tar_tables(list): 对数据源中的哪些表进行关系发现，如果未指定，则对全部表进行查找
        custom_para(tuple): 用户配置的参数组成的元组
        **db_kw: 目标数据源的相关参数

    Returns:

    """
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
    host = db_kw['host']
    port = db_kw['port']
    user = db_kw['user']
    password = db_kw['passwd']
    database = db_kw['url'].split('/')[-1]
    conn = connect(host=host, port=port, user=user, password=password, database=database,
                   auth_mechanism='PLAIN')
    table_and_comments = {}
    if not tar_tables:
        logger.info('用户未指定表，将读取目标库中的全表进行计算')
        with conn.cursor() as cr:
            sql = 'show tables'
            cr.execute(sql)
            tables = [i[0] for i in cr.fetchall()]
    else:
        logger.info('用户指定了表，将在指定表中寻找关联关系')
        tables = tar_tables
    for tab in tables:
        with conn.cursor() as cr:
            try:
                sql = f'show create table {tab}'
                cr.execute(sql)
                res = cr.fetchall()
                for i in res:
                    s = i[0]
                    if s.startswith("COMMENT"):
                        break
                comment = s.split("'")[1]
            except:
                comment = ''
            table_and_comments[tab] = comment

    if not table_and_comments:
        logger.warning('未发现可用表')
        return
    logger.info(f'获取表成功，共：{len(table_and_comments)}个')

    if multi_process:
        if len(table_and_comments) < 150:
            logger.warning(f'表数较少（{len(table_and_comments)}），启用多进程可能效果不佳')
    df = execute(model_id, processes, table_and_comments, custom_para, **db_kw)
    return df


def execute(model_id, processes, table_and_comments, custom_para=None, **kwargs):
    """执行函数。

    Args:
        model_id(str): 当前融合任务的唯一标识
        processes: 进程数量
        table_and_comments(dict): 所有待计算的表名
        custom_para(tuple): 用户配置的参数组成的元组
        **kwargs: 目标数据源的相关参数

    Returns:

    """
    logger = logging.getLogger(f'{model_id}')
    use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len = custom_para
    use_str_len = int(use_str_len)
    inf_dup_ratio = float(inf_dup_ratio)
    inf_str_len = int(inf_str_len)
    inf_tab_len = int(inf_tab_len)

    host, port, user = kwargs['host'], int(kwargs['port']), kwargs['user']
    password, database = kwargs['passwd'], kwargs['url'].split('/')[-1]
    if processes == 1:  # 单进程
        rel_cols, pks = pre_processing(model_id, table_and_comments, False, host,
                                       port, user, password, database, data_cleansing, inf_tab_len)
        output = find_rel(rel_cols, pks, model_id, False, host,
                          port, user, password, database, use_str_len, inf_dup_ratio, inf_str_len)
    else:
        logger.info('多进程预处理数据...')
        if not len(table_and_comments) % processes:
            batch_size = int(len(table_and_comments) / processes)
        else:
            batch_size = int(len(table_and_comments) / processes) + 1
        jobs = []
        for i in range(processes):
            if i == processes - 1:
                p = multiprocessing.Process(target=pre_processing, name=f'preprocess-Process-{i}',
                                            args=(model_id, table_and_comments[i * batch_size:],
                                                  True, host, port, user, password, database,
                                                  data_cleansing, inf_tab_len,))
            else:
                p = multiprocessing.Process(target=pre_processing, name=f'preprocess-Process-{i}',
                                            args=(model_id, table_and_comments[i * batch_size: (i + 1) * batch_size],
                                                  True, host, port, user, password, database,
                                                  data_cleansing, inf_tab_len,))
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
            logger.info(f'{p.name} join 完成')
        rel_cols, pks = {}, {}
        file_list = [i for i in os.listdir(f'./caches/{model_id}') if i.startswith('preprocess-Process')]
        for file in file_list:
            with open(f'./caches/{model_id}/{file}') as f:
                res = json.load(f)
            rel_cols.update(res['rel_cols'])
            pks.update(res['pks'])

        logger.info('多进程数据预处理完成')

        logger.info('多进程关系发现...')
        jobs = []
        rel_cols_items = list(rel_cols.items())
        if not len(rel_cols_items) % processes:
            batch_size = int(len(rel_cols_items) / processes)
        else:
            batch_size = int(len(rel_cols_items) / processes) + 1
        for i in range(processes):
            if i == processes - 1:
                p = multiprocessing.Process(target=find_rel, name=f'rel-Process-{i}',
                                            args=(rel_cols_items[i * batch_size:], pks,
                                                  model_id, True, host, port, user, password, database,
                                                  use_str_len, inf_dup_ratio, inf_str_len,))
            else:
                p = multiprocessing.Process(target=find_rel, name=f'rel-Process-{i}',
                                            args=(rel_cols_items[i * batch_size: (i + 1) * batch_size], pks,
                                                  model_id, True, host, port, user, password, database,
                                                  use_str_len, inf_dup_ratio, inf_str_len,))
            jobs.append(p)
            p.start()
        output = []
        for p in jobs:
            p.join()
            logger.info(f'{p.name} join 完成')

        file_list = [i for i in os.listdir(f'./caches/{model_id}') if i.startswith('rel-Process')]
        for file in file_list:
            with open(f'./caches/{model_id}/{file}') as f:
                res = json.load(f)
            output.extend(res['rel'])

    columns = ['model1', 'db1', 'table1', 'table1comment', 'column1',
               'column1comment', 'model2', 'db2', 'table2', 'table2comment',
               'column2', 'column2comment', 'matching_degree']
    if output:
        df = pd.DataFrame(columns=columns, data=output)
        logger.info('表关系查找完成')
        return df
    else:
        logger.info('未找到关系')
        return pd.DataFrame(columns=columns, data=[])


def pre_processing(model_id, table_and_comments, multi, host, port, user, password, database,
                   data_cleansing=None, inf_tab_len=None):
    """获取table的主键和可能的外键，并针对主键生成filter文件。

    Args:
        model_id(str): 当前融合任务的唯一标识
        table_and_comments(dict): 表名列表
        multi(bool): 是否采用多进程进行计算
        host(str): 目标数据库的ip
        port(int): 目标数据库的端口号
        user(str): 目标数据库的用户名
        password(str): 目标数据库的密码
        database(str): 目标数据源的数据库名称
        data_cleansing(dict): 包含过滤规则的字典
        inf_tab_len(int): 表长度下界

    Returns:
        rel_cols: 一个字典，键为表名，值为该张表可能作为外键的字段列表
        pks: 一个字典，键为表名，值为该张表可能作为主键的字段列表

    """
    if inf_tab_len is None:
        inf_tab_len = 0
    if not multi:
        logger = logging.getLogger(f'{model_id}')
    else:
        logger = sub_process_logger(model_id, multiprocessing.current_process().name)
        logger.info(f"""
        本子进程中需要处理的表总数为{len(table_and_comments)}
        """)
    conn = connect(host=host, port=port, user=user, password=password, database=database,
                   auth_mechanism='PLAIN')
    sql1 = f'select count(1) from `{database}`.`%s`'
    sql2 = f'desc `{database}`.`%s`'
    sql3 = f'select count(`%s`) from `{database}`.`%s`'
    sql4 = f'select count(distinct `%s`) from `{database}`.`%s`'
    sql5 = f'select count(1) from `{database}`.`%s` where length(`%s`)=octet_length(`%s`)'
    cr = conn.cursor()
    rel_cols = {}  # 存储表及其可能与其他表主键进行关联的字段
    length_normal = {}  # 存储表及其长度
    length_too_long = {}  # 存储表及其长度；行数太长，可能导致内存溢出无法计算
    length_zero = []  # 空表
    without_pks = []  # 没有主键的表
    pks = {}  # 存储表及其可能的主键列表
    no_exist = []  # 数据库中不存在的表
    logger.info('预处理所有表')
    i = 0
    for tab in table_and_comments:
        tab_comment = table_and_comments[tab]
        logger.debug(f'进度：{i+1}/{len(table_and_comments)}')
        logger.debug(f'  {tab}：长度校验')
        try:
            cr.execute(sql1 % tab)
            row_num = cr.fetchone()[0]
            if row_num > 1e8:
                length_too_long[tab] = row_num
                logger.debug(f'  {tab}：超长，被过滤')
                i += 1
                continue
            elif row_num == 0:
                length_zero.append(tab)
                logger.debug(f'  {tab}：为空，被过滤')
                i += 1
                continue
            elif row_num < int(inf_tab_len):
                logger.debug(f'  {tab}表的长度低于设置的阈值（{inf_tab_len}）')
                i += 1
                continue
            else:
                length_normal[tab] = row_num
                logger.debug(f'  {tab}：长度合格')
        except Exception as e:
            logger.debug(f'  {tab}：不存在：{e}')
            no_exist.append(tab)
            i += 1
            continue

        logger.debug(f'  {tab}：查找主键')
        try:
            cr.execute(sql2 % tab)
        except:
            logger.warning('数据库内部错误')
            logger.warning(traceback.format_exc())
            i += 1
            continue

        field_info = cr.fetchall()
        psb_pk, psb_col = {}, {}
        for j in range(len(field_info)):
            field_name = field_info[j][0]
            field_type = field_info[j][1]
            field_comment = field_info[j][2]
            if not col_name_filter(tab, field_name, data_cleansing):
                logger.debug(f'    {field_name}不符合保留规则，被过滤')
                continue
            # if field_type.upper() not in mysql_type_list:
            #     logger.debug(f'    {field_name}的数据类型是{field_type}，不属于要计算的数据类型{mysql_type_list}')
            #     continue
            try:
                cr.execute(sql3 % (field_name, tab))
            except:
                logger.warning('数据库内部错误')
                logger.warning(traceback.format_exc())
                continue
            num1 = cr.fetchone()[0]
            if num1 == row_num:
                try:
                    cr.execute(sql4 % (field_name, tab))
                    num2 = cr.fetchone()[0]
                    try:  # 非字符型无法使用对应函数
                        cr.execute(sql5 % (tab, field_name, field_name))
                        num3 = cr.fetchone()[0]
                    except:
                        num3 = num2
                except:
                    logger.warning('数据库内部错误')
                    logger.warning(traceback.format_exc())
                    continue
                if num1 == num2 and num2 == num3:
                    psb_pk[field_name] = field_comment
                    if both_roles:
                        psb_col[field_name] = field_comment
                elif num1 == num3:
                    psb_col[field_name] = field_comment
        rel_cols[tab] = {'comment': tab_comment,
                         'psb_col': psb_col}
        if len(psb_pk):
            logger.debug(f'  {tab}：主键已保存')
            pks[tab] = {'comment': tab_comment, 'psb_pk': psb_pk}
        else:
            logger.debug(f'  {tab}：未发现主键')
            without_pks.append(tab)
            i += 1
            continue

        logger.debug(f'  {tab}：正在生成filter文件')
        if not os.path.exists(f'./filters/{model_id}/{database}'):
            os.makedirs(f'./filters/{model_id}/{database}')
        capacity = int(length_normal[tab] * 1.2)
        for pk in pks[tab]['psb_pk']:
            filter_name = tab + '@' + pk + '.filter'
            if os.path.exists(f'./filters/{model_id}/{database}/{filter_name}'):
                logger.debug(f'    {tab}.{pk} 已经存在')
                continue
            value = pd.read_sql(f"select `{pk}` from `{tab}`", conn)
            bf = BloomFilter(capacity)
            for j in value.iloc[:, 0]:
                bf.add(j)
            with open(f'./filters/{model_id}/{database}/{filter_name}', 'wb') as f:
                pickle.dump(bf, f)
        logger.debug(f'  {tab}：全部filter已保存')
        i += 1
    logger.info('完成')
    if multi:
        cache_file_name = multiprocessing.current_process().name + '.json'
        if not os.path.exists(f'./caches/{model_id}'):
            os.makedirs(f'./caches/{model_id}')
        res = {'rel_cols': rel_cols, 'pks': pks}
        with open(f'./caches/{model_id}/{cache_file_name}', 'w') as f:
            json.dump(res, f)
    cr.close()
    conn.close()
    return rel_cols, pks


def find_rel(rel_cols, pks, model_id, multi, host, port, user, password, database,
             use_str_len=None, inf_dup_ratio=None, inf_str_len=None):
    """查找关系。

    Args:
        rel_cols(dict): 一个字典，键为表名，值为该张表可能作为外键的字段列表
        pks(dict): 一个字典，键为表名，值为该张表所有的可作为主键的字段列表
        model_id(str): 当前融合任务的唯一标识
        multi(bool): 是否采用多进程进行计算
        host(str): 目标数据库的ip
        port(int): 目标数据库的端口号
        user(str): 目标数据库的用户名
        password(str): 目标数据库的密码
        database(str): 目标数据源的数据库名称
        use_str_len(int): 以整型代替布尔值，表示是否使用字符平均长度来过滤
        inf_dup_ratio(float): 去重后的列表长度占原列表长度的比例
        inf_str_len(int): 将值转化为字符后的平均长度，仅当use_str_len生效时生效

    Returns:

    """
    use_str_len = 0 if use_str_len is None else use_str_len
    inf_dup_ratio = 0.0 if inf_dup_ratio is None else inf_dup_ratio
    inf_str_len = 0 if inf_str_len is None else inf_str_len

    if not multi:
        logger = logging.getLogger(model_id)
    else:
        logger = sub_process_logger(model_id, multiprocessing.current_process().name)
        logger.info(f"""本子进程所需要处理的表总数共{len(rel_cols)}""")
    rel_cols_dict = {}
    if isinstance(rel_cols, list):
        for i in rel_cols:
            rel_cols_dict[i[0]] = i[1]
    else:
        rel_cols_dict = rel_cols

    sql = f'select `%s` from `{database}`.`%s` limit 10000'
    results = []
    conn = connect(host=host, port=port, user=user, password=password, database=database,
                   auth_mechanism='PLAIN')
    logger.info('计算所有关系')
    i = 1
    for tab in rel_cols_dict:
        logger.debug(f'进度：{i}/{len(rel_cols_dict)}')
        for col in rel_cols_dict[tab]['psb_col']:
            value = pd.read_sql(sql % (col, tab), conn)
            df = col_value_filter(value, int(use_str_len), int(inf_str_len), float(inf_dup_ratio))
            if (isinstance(df, pd.DataFrame) and df.empty) or (df is None):
                logger.debug(f'{tab}表的{col}字段值未通过内容校验')
                continue
            for pk_tab in pks:
                if pk_tab == tab:
                    continue
                for pk in pks[pk_tab]['psb_pk']:
                    with open(f'./filters/{model_id}/{database}/{pk_tab}@{pk}.filter', 'rb') as f:
                        bf = pickle.load(f)
                    flag = 1
                    num_not_in_bf = 0
                    for k in df[col]:
                        if k not in bf:
                            num_not_in_bf += 1
                        if num_not_in_bf / 10000 > sup_out_foreign_key:
                            flag = 0
                            break
                    if flag:
                        not_match_ratio = num_not_in_bf / 10000
                        res = [model_id, database, pk_tab, pks[pk_tab]['comment'], pk, pks[pk_tab]['psb_pk'][pk],
                               model_id, database,
                               tab, rel_cols_dict[tab]['comment'], col, rel_cols_dict[tab]['psb_col'][col],
                               not_match_ratio]
                        results.append(res)
        i += 1
    if multi:
        file_name = multiprocessing.current_process().name + '.json'
        res = {'rel': results}
        with open(f'caches/{model_id}/{file_name}', 'w') as f:
            json.dump(res, f)
    conn.close()
    logger.info('完成')
    return results

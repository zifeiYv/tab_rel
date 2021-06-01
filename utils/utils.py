# -*- coding: utf-8 -*-
import pymysql
import logging
from logging.handlers import RotatingFileHandler
import os
import uuid
import time
import shutil
import traceback
import json


def init_logger(model_id):
    if not os.path.exists(f'./logs/{model_id}'):
        os.makedirs(f'./logs/{model_id}')

    log_dir = f'./logs/{model_id}'
    formatter = logging.Formatter('%(asctime)s %(levelname)7s %(filename)8s line %(lineno)4d | %(message)s ',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logger_name = f'{model_id}'
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler0 = RotatingFileHandler(log_dir + '/' + logger_name + '-debug.log', maxBytes=10 * 1024 * 1024,
                                   backupCount=5)
    handler1 = RotatingFileHandler(log_dir + '/' + logger_name + '-info.log', maxBytes=10 * 1024 * 1024,
                                   backupCount=5)
    handler2 = RotatingFileHandler(log_dir + '/' + logger_name + '-warn.log', maxBytes=10 * 1024 * 1024,
                                   backupCount=5)
    handler0.setLevel(logging.DEBUG)
    handler1.setLevel(logging.INFO)
    handler2.setLevel(logging.WARNING)
    handler0.setFormatter(formatter)
    handler1.setFormatter(formatter)
    handler2.setFormatter(formatter)
    logger.addHandler(handler0)
    logger.addHandler(handler1)
    logger.addHandler(handler2)


def get_logger(logger_name):
    """初始化一个日志记录器，无返回值。

    只需要在开始执行函数前进行初始化，在整个函数执行过程中可以直接根据logger_name从标准库获取logger对象
    进行日志记录即可。
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    handler1 = RotatingFileHandler('./logs/'+logger_name+'-info.log', maxBytes=5 * 1024 * 1024, backupCount=5)
    handler2 = RotatingFileHandler('./logs/'+logger_name+'-err.log', maxBytes=5 * 1024 * 1024, backupCount=5)
    handler2.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s %(levelname)7s %(filename)8s line %(lineno)4d | %(message)s ',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler1.setFormatter(formatter)
    handler2.setFormatter(formatter)
    logger.addHandler(handler1)
    logger.addHandler(handler2)


def filter_str(col, data_cleansing):
    """Decide whether `col` is useful basing on `data_cleansing`.

    Args:
        col(str): Column name.
        data_cleansing(dict): A dict that contains filter rules.

    Returns:
        col or None.
    """
    if not data_cleansing:
        return None
    for v in data_cleansing['_']:
        if v.endswith('%'):
            if col == v[:-1]:
                return col
            else:
                return None
        else:
            if col.startswith(v):
                return None
            else:
                return col


def col_name_filter(tab, col, data_cleansing=None):
    """Decide whether the `col` in `tab` is useful basing on `data_cleansing`.
    The format of dict `data_cleansing` is defined as follow:
        {
            "_": ["str1", "str2", ...],
            "tab1": ["tab1_str1", "tab1_str2", ...],
            ...
        }
    The values of key `_` is used for all tables and values of `tab*`(a specific table name) is used only for that
    table.
    All strings in values are either end with "%" or not. A string is ends with "%" means that `col` will be filtered
    only if `col`` is exactly the same with characters before "%"; otherwise, `col` will be filtered if it starts
    with that string.

    Notes:
        Key "_" must exist.

    Args:
        tab(str): Table name.
        col(str): Column name.
        data_cleansing(dict): A dict that contains filter rules.

    Returns:
        col or None.
    """
    if not data_cleansing:
        return col
    if tab not in data_cleansing:
        return filter_str(col, data_cleansing)
    else:
        if not filter_str(col, data_cleansing):
            return None
        else:
            for v in data_cleansing[tab]:
                if v.endswith('%'):
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


def col_value_filter(df, use_str_len, inf_str_len, inf_dup_ratio):
    """Decide whether the value in `df` is useful basing on the users' config.

    Args:
        df: A data frame.
        use_str_len(bool or int): Boolean value, whether the average length of each value, treated as string, is
            used to filter `df`.
        inf_str_len(int): The minimum value of average length of string-ed value.
        inf_dup_ratio: The length of duplicated `df`

    Returns:

    """
    if use_str_len:
        avg_len = df.iloc[:, 0].astype('str').str.len().mean()
    else:
        avg_len = 999
    if avg_len < inf_str_len:
        return None
    else:
        null_num = len(df[df.iloc[:, 0].isin([None, ''])])
        if sum(-df.duplicated()) < df.shape[0] * inf_dup_ratio:
            return None
        elif null_num / len(df) > .8:
            return None
        else:
            return df


def save_to_db(output, conn, last_rel_res):
    """Insert `output` results to database.

    Args:
        output: A data frame contains relation results.
        conn: A config dict for target database.
        last_rel_res: Table relation of last computation.

    Returns:

    """
    cr = conn.cursor()
    num_new_rel = 0
    for i in range(output.shape[0]):
        _id = str(uuid.uuid1()).replace('-', '')
        line = output.iloc[i]
        model_id = line['model1']
        db1 = line['db1']
        table1 = line['table1']
        table1comment = line['table1comment'].replace("\'", "\\'").replace("\"", "\\")
        column1 = line['column1']
        column1comment = line['column1comment'].replace("\'", "\\'").replace("\"", "\\")
        db2 = line['db2']
        table2 = line['table2']
        table2comment = line['table2comment'].replace("\'", "\\'").replace("\"", "\\")
        column2 = line['column2']
        column2comment = line['column2comment'].replace("\'", "\\'").replace("\"", "\\")
        not_match_ratio = line['matching_degree']
        if (db1 + table1 + column1 + db2 + table2 + column2) in last_rel_res:
            status = 0
        else:
            status = 1
            num_new_rel += 1
        insert_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        sql = f'insert into analysis_results(`id`, `model`, `db1`, `table1`, `table1_comment`, ' \
              f'`column1`, `column1_comment`, `db2`, `table2`, `table2_comment`, `column2`, ' \
              f'`column2_comment`, `status`, `scantype`,`create_time`,`edit_time`, `matching_degree`) values (' \
              f'"{_id}", "{model_id}", "{db1}", "{table1}", "{table1comment}", "{column1}", "{column1comment}", ' \
              f' "{db2}", "{table2}","{table2comment}", "{column2}","{column2comment}","{status}", "0", ' \
              f'"{insert_time}", "{insert_time}", "{not_match_ratio}")'
        cr.execute(sql)
    conn.commit()
    cr.close()
    return num_new_rel


def del_cache_files(post_json):
    """Delete cached files, called when updating parameters"""
    db_name = post_json['db']
    filter_path = f'./filters/{db_name}'
    attr_path = f'./table_attr/{db_name}'
    try:
        if os.path.isdir(filter_path):
            shutil.rmtree(filter_path)
        if os.path.isdir(attr_path):
            shutil.rmtree(attr_path)
    except Exception as e:
        print(e)
        return 0
    return 1


def sub_process_logger(model_id, process_name):
    log_dir = f'./logs/{model_id}'
    formatter = logging.Formatter('%(asctime)s %(levelname)7s %(filename)8s line %(lineno)4d | %(message)s ',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logger_name = process_name
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler0 = RotatingFileHandler(log_dir + '/' + logger_name + '-debug.log', maxBytes=10 * 1024 * 1024,
                                   backupCount=5)
    handler1 = RotatingFileHandler(log_dir + '/' + logger_name + '-warn.log', maxBytes=10 * 1024 * 1024,
                                   backupCount=5)
    handler0.setFormatter(formatter)
    handler1.setLevel(logging.WARNING)
    handler1.setFormatter(formatter)
    logger.addHandler(handler1)
    logger.addHandler(handler0)
    return logger

# -*- coding: utf-8 -*-
import logging
from logging.handlers import RotatingFileHandler
import os
import uuid
import time


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


def filter_str(col, data_cleansing):
    """根据data_cleansing来判断col是否应该被保留。

    Args:
        col(str): 字段名
        data_cleansing(dict): 过滤规则

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
    """根据data_cleansing判断tab表的col字段是否应该被保留。

    data_cleansing的格式如下
        {
            "_": ["str1", "str2", ...],
            "tab1": ["tab1_str1", "tab1_str2", ...],
            ...
        }

    键"_"对应的值（规则）适用于所有表；其他的键对应的值（规则）只适用于对应表。

    所有的值（规则）均是字符串，并且只有两种格式：以"%"结尾，或不以"%"结尾。以"%"结尾表示col必须与该规则完全一致
    才会被过滤；否则，只要col以该规则字符串开头即被过滤。

    Notes:
        键"_"及其对应的规则必须存在。

    Args:
        tab(str): 表名
        col(str): 字段名
        data_cleansing(dict): 规律规则

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
    """根据用户自定义参数来判断df中的数据是否被利用。

    Args:
        df: A data frame.
        use_str_len(bool or int): 是否需要将df中的每个值视为字符串，然后计算其平均字符长度
        inf_str_len(int): 当df被保留时，其中的值的平均字符长度的最小值
        inf_dup_ratio(float): 当df被保留时，其中的无重复的值占总数的最小比例

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
    """将output存入数据库"""
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

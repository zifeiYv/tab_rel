# -*- coding: utf-8 -*-
"""
@Time       : 2020/2/8 13:26
@Author     : Jarvis
@Annotation : Sorry for this shit code

The database server where results table and status table in is MySQL. Table names are `pf_analysis_result1` and
`pf_analysis_status` and not self-specified. In general, they will be created when initializing the system.

The definition of above tables are as follows:

    SET FOREIGN_KEY_CHECKS=0;
    -- ----------------------------
    -- Table structure for pf_analysis_result1
    -- ----------------------------
    DROP TABLE IF EXISTS `pf_analysis_result1`;
    CREATE TABLE `pf_analysis_result1` (
      `id` varchar(255) NOT NULL,
      `id1` varchar(255) DEFAULT NULL,
      `id2` varchar(255) DEFAULT NULL,
      `linkageid` varchar(255) DEFAULT NULL,
      `table1` varchar(255) DEFAULT NULL,
      `table2` varchar(255) DEFAULT NULL,
      `column1` varchar(40) DEFAULT NULL,
      `column2` varchar(40) DEFAULT NULL,
      `db1` varchar(40) DEFAULT NULL,
      `db2` varchar(40) DEFAULT NULL,
      `model1` varchar(40) DEFAULT NULL,
      `model` varchar(64) DEFAULT NULL,
      `model2` varchar(40) DEFAULT NULL,
      `remark` varchar(40) DEFAULT NULL,
      `scantype` int(11) DEFAULT NULL,
      `status` int(11) DEFAULT NULL,
      `column1_comment` varchar(128) DEFAULT NULL COMMENT '字段1中文名称',
      `column2_comment` varchar(128) DEFAULT NULL COMMENT '字段2中文名称',
      `create_time` datetime DEFAULT NULL COMMENT '创建时间',
      `edit_time` datetime DEFAULT NULL COMMENT '编辑时间',
      `incidence` varchar(0) DEFAULT NULL COMMENT '备注表说明',
      `label` varchar(0) DEFAULT NULL COMMENT '备注表标签',
      `table1_comment` varchar(128) DEFAULT NULL COMMENT '表1中文名称',
      `table2_comment` varchar(128) DEFAULT NULL COMMENT '表1中文名称',
      `matching_degree` float DEFAULT NULL COMMENT '匹配度',
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    SET FOREIGN_KEY_CHECKS=0;
    -- ----------------------------
    -- Table structure for pf_analysis_status
    -- ----------------------------
    DROP TABLE IF EXISTS `pf_analysis_status`;
    CREATE TABLE `pf_analysis_status` (
      `linkageid` varchar(255) NOT NULL,
      `analysisstatus` varchar(255) DEFAULT NULL,
      `algorithmname` varchar(64) DEFAULT NULL COMMENT '算法名称',
      `end_time` datetime DEFAULT NULL COMMENT '算法结束时间',
      `executobj` varchar(64) DEFAULT NULL COMMENT '执行对象',
      `new_relation_num` int(5) DEFAULT NULL COMMENT '新增关系数量',
      `relationnum` int(5) DEFAULT NULL COMMENT '总关系数量',
      `start_time` datetime DEFAULT NULL COMMENT '算法开始时间',
      PRIMARY KEY (`linkageid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

"""
import pymysql
import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import os
import uuid
import time
import shutil


def check_parameters(parameters):
    """Check the post parameters is valid or not before following calculation.

    Args:
        parameters(dict): Parameter in json-format(dict object in python).

    Returns:
        True or wrong messages.
    """
    if 'modelId1' not in parameters:
        return '`modelId1` is required while not included.'
    elif not isinstance(parameters['modelId1'], str):
        return 'The value type of `modelId1` must be string.'
    elif 'modelId2' not in parameters:
        return '`modelId2` is required while not included.'
    elif not isinstance(parameters['modelId2'], str):
        return 'The value type of `modelId2` must be string.'
    elif 'configMap' not in parameters:
        return '`configMap` is required while not included.'
    elif not isinstance(parameters['configMap'], dict):
        return 'The value type of `configMap` must be dict.'
    elif not isinstance(parameters['algorithmName'], str):
        return 'The value type of `algorithmName` must be str'
    elif not isinstance(parameters['executObj'], str):
        return 'The value type of `executObj` must be str'
    else:
        for i in ['host', 'port', 'user', 'password', 'db']:
            if i not in parameters['configMap']:
                return f'`{i}` is required in `configMap` while not included.'
        return True


def check_connection(config):
    """The computation results are saved in a specific table of a MySQL database. So the
    connection to database must be worked before following calculation.

    Args:
        config(dict): A dict that contains all information for database connection.

    Returns:
        A `pymysql.connections.Connection` object or error messages.
    """
    try:
        connection = pymysql.connect(**config)
        return connection
    except Exception as e:
        print(e)  # Print this to console may help since it doesn't logged.
        return e


def gen_logger(log_file_name, console_print=True):
    """This function is used to generate an object that can log the key messages of calculation.
    All log files are stored in `../logs/`(will be created for the first time) directory and
    the file name is corresponding with model ids.

    Args:
        log_file_name(str): Log file name.
        console_print(bool): If print log in console, default is `True`.

    Returns:
        An object to log key messages.
    """
    cwd = os.getcwd()
    logs_dir = os.path.join(cwd, 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    log_file_name = os.path.join(logs_dir, log_file_name)

    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        console = StreamHandler()
        handler = RotatingFileHandler(log_file_name, maxBytes=5*1024*1024, backupCount=5)
        formatter = logging.Formatter('%(asctime)s %(levelname)s | %(message)s | <'
                                      '%(filename)s %(funcName)s line %(lineno)d>', datefmt='%Y-%m-%d %H:%M:%S')
        # formatter = logging.Formatter('%(asctime)s %(levelname)s | %(message)s ', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        console.setFormatter(formatter)
        logger.addHandler(handler)
        if console_print:
            logger.addHandler(console)
    return logger


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
        elif null_num/len(df) > .8:
            return None
        else:
            return df


def res_to_db(output, config, last_rel_res, log):
    """Insert `output` results to database.

    Args:
        output: A data frame contains relation results.
        config(dict): A config dict for target database.
        last_rel_res: Table relation of last computation.
        log: A log object.

    Returns:

    """
    conn = pymysql.connect(**config)
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
        try:
            cr.execute(sql)
        except Exception as e:
            log.error(e)
            continue
    conn.commit()
    cr.close()
    conn.close()
    return num_new_rel


def roll_back(status_bak, conn, model_id):
    """This roll back operation is designed for table 'analysis_status' to make sure the 'analysisstatus' field back
     to its original value if some errors occur in computation.

    Args:
        status_bak: `False` if it doesn't a re-computation model or original status value(almost it is '2').
        conn: A database connection object from `pymysql` module.
        model_id(str): The model's unique identification.
    Returns:
        None
    """
    with conn.cursor() as cr:
        if not status_bak:  # A first-computation model and delete its record directly.
            cr.execute(f'delete from analysis_status where id="{model_id}"')
        else:  # A re-computation model.
            cr.execute(f'update analysis_status set analysis_status="{status_bak}" where id="{model_id}"')
        conn.commit()


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

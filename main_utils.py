# -*- coding: utf-8 -*-
"""
@Time       : 2020/2/10 14:13
@Author     : Jarvis
@Annotation : Sorry for this shit code
"""
from utils import check_connection, gen_logger
from utils import col_name_filter, col_value_filter, res_to_db, roll_back, res_to_db2
from pymysql.connections import Connection
import pymysql
import time
import json
import os
import requests
import redis
import pickle
import pandas as pd
import traceback
# noinspection PyPackageRequirements
from pybloom import BloomFilter
from config import both_roles, not_cite_table, not_base_table, sup_out_foreign_key, \
    multi_process, redis_config, mysql_type_list, oracle_type_list, pg_type_list

if multi_process:
    from faster import add_operation

not_base_table_list = [] if not not_base_table else list(not_base_table.split(','))
not_cite_table_list = [] if not not_cite_table else list(not_cite_table.split(','))


class R:
    """如果没有安装redis环境或者配置错误导致无法连接，那么则实例化这个类，以避免进度条不可用时相关方法报错"""
    def get(self, name):
        pass

    def set(self, name, value):
        pass


try:
    r = redis.Redis(**redis_config)
    r.ping()
except Exception:
    print(traceback.print_exc())
    r = R()


def main_process(post_json):
    """计算的主过程。

    Args:
        post_json(dict): 用户POST的参数

    Returns:

    """
    global r
    r.set('stage', '初始化')  # 计算的阶段
    r.set('progress', 0)  # 当前阶段的进度，取值为0-100
    r.set('msg', '')  # 额外的信息
    r.set('state', 0)  # 计算的状态, 0：计算中, 1：计算完成(或出错)

    # Check if the config database is connectable.
    config_map = post_json['configMap']
    conn = check_connection(config_map)
    if not isinstance(conn, Connection):
        msg = '在连接配置数据库时出现一个错误'
        r.set('progress', 100)
        r.set('state', 1)
        r.set('msg', msg)
        return {'state': 0, 'msg': msg}
    # Get all parameters and initialize a log object.
    model_id = post_json['modelId']
    alg_name = post_json['algorithmName']
    exe_obj = post_json['executObj']
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logging = gen_logger(model_id)
    logging.info(f'{"*" * 80}')
    logging.info('所有参数处理完成，开始进行计算。')
    logging.info(f'{"*" * 80}')
    logging.info(f'model ID : {model_id}')
    if multi_process:
        logging.warning('Multi process mode')
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # 初始化模型的状态表。
    # 注意:
    #   - 对于一个新建的模型，结果表中是不存在旧结果的，因此，程序会在状态表中插入一条新记录，字段
    #   "analysisstatus"的值为1（代表"计算中"），并且在计算完成后将这个值改写为2（代表"计算完成"）。
    #   如果在计算过程中出现来一些错误，那么程序会删除这一条新增的记录。
    #   - 对于一个已经计算过关系的模型，结果表中可能存在旧结果，因此，重新计算时，程序会将旧结果全部删除（用户
    #   指定的某些结果会得到保留）。另外，由于已经计算过，所以状态表中字段"analysisstatus"的值为2，程序首先
    #   将其改写为1，如果计算完成，再改写为1。如果过程中出错，那么将把该值重新恢复成2。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    logging.info('初始化状态表...')
    r.set('progress', 30)

    sql = f'REPLACE INTO analysis_status (id, analysis_status, algorithm_name, execute_obj,' \
          f'start_time) values("{model_id}", "1", "{alg_name}", "{exe_obj}", "{start_time}")'
    try:
        with conn.cursor() as cr:
            # Cache the status of model for rollback when needed
            cr.execute(f"select t.analysis_status from analysis_status t where t.id='{model_id}'")
            res = cr.fetchone()
            if not res:
                status_bak = False
                logging.info('模型首次参与运行，回滚操作时将会删除状态表中的记录。')
            else:
                status_bak = res[0]
                logging.info('模型已经存在运算结果，回滚操作将会把状态值改写为2。')
            # Change the status to 'analyzing'
            cr.execute(sql)
            conn.commit()
            r.set('progress', 100)
    except Exception as e:
        print(traceback.print_exc())
        logging.error(e)
        logging.info('开始回滚...')
        try:
            roll_back(status_bak, conn, model_id)
            logging.info('回滚完成')
        except Exception as e:
            # This error is due to the undefined of `status_bak`.
            logging.info('回滚出错')
            logging.error(e)
        r.set('progress', 100)
        r.set('state', 1)
        return {'state': 0, 'msg': '初始化状态表时出错'}
    logging.info('初始化完成')
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #   "custom parameters"记录了一些临界值，这些临界值对于计算结果的准确性是有非常大的影响的。用户在充分理解
    #   各个值的作用的情况下，调整这些值有利于使结果更加合理。
    #   如果用户不加修改，那么会采用默认值。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    r.set('stage', '参数获取')  # Stage of calculation
    r.set('progress', 0)  # Progress, value between 0 and 100
    r.set('msg', '')  # Additional message
    logging.info('获取用户参数...')
    sql = f'select t.csl, t.ds, t.idr, t.isl, t.itl, t.tables1, t.tables2 from ' \
          f'pf_user_config t where t.model="{model_id}{model_id}"'
    try:
        with conn.cursor() as cr:
            if cr.execute(sql):
                logging.info('获取用户参数成功。')
                res = cr.fetchone()
                use_str_len = str(res[0]) if res[0] else '0'
                data_cleansing = eval(str(res[1])) if res[1] else {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = res[2] if res[2] else 0.4
                inf_str_len = res[3] if res[3] else 3
                inf_tab_len = res[4] if res[4] else 10
                tables1 = list(res[5].split(',')) if res[5] else []
            else:
                logging.warning('当前未指定参数，将采用默认值。')
                use_str_len = '0'
                data_cleansing = {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = 0.4
                inf_str_len = 3
                inf_tab_len = 10
                tables1 = []
            # Merge custom parameters to a tuple
            custom_para = (use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len)
            r.set('progress', 100)
    except Exception as e:
        logging.error(e)
        logging.info('开始回滚...')
        roll_back(status_bak, conn, model_id)
        logging.info('回滚成功')
        r.set('state', 1)
        r.set('msg', '回滚出错')
        r.set('progress', 100)
        return {'state': 0, 'msg': '回滚过程出错'}
    logging.info('完成参数获取')

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #   初始化完成后，程序将从配置文件中获取数据源的相关信息，开始读取数据进行计算。
    #   目前，只支持从单一数据源获取数据进行计算。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    r.set('stage', '计算关系1/3')
    r.set('progress', 0)
    data_source = post_json['dbInfo']
    data_source['model_id'] = model_id
    data_source['tables'] = tables1

    finish_url = post_json['notifyUrl']
    finish_url += f'?modelId={model_id}'

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #   如果模型并非首次进行运算，那么在写入新的结果前会把部分旧的结果删除。
    #   如果旧的结果是用户手动添加的或者经过了用户的编辑或是用户删除的数据，那么在结果表中的"scantype"字段
    #   会以"1"标示；否则，以"0"标示。在执行删除操作时，只会删除"scantype"取值为"0"的记录。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    last_rel_res = []  # 缓存上次计算的结果
    user_rel_res = []  # 缓存经过用户标记的结果
    if status_bak:  # a re-compute model
        try:
            logging.info('加载上次计算结果...')
            with conn.cursor() as cr:
                cr.execute(
                    f'select db1, table1, column1, db2, table2, column2 from analysis_results '
                    f'where '
                    f'model="{model_id}"')
                res = cr.fetchall()
                for r_ in res:
                    last_rel_res.append("".join(r_))
                cr.execute(
                    f'select db1, table1, column1, db2, table2, column2 from analysis_results '
                    f'where '
                    f'model="{model_id}" and `scantype` != 0')
                res = cr.fetchall()
                for r_ in res:
                    user_rel_res.append("".join(r_))
            conn.commit()
            logging.info('加载结果完成')
        except Exception as e:
            logging.error(e)
            logging.info('开始回滚...')
            roll_back(status_bak, conn, model_id)
            logging.info('回滚出错')
            return {'state': 0, 'msg': '加载旧结果时出错。'}

    output = one_db(data_source, logging, custom_para, user_rel_res)

    if output.empty:
        r.set('state', 1)
        r.set('msg', 'Empty output')
        logging.info('结果为空')
        roll_back(status_bak, conn, model_id)
        requests.get(finish_url)
        logging.info(f'计算完成')
    else:
        # 删除旧版结果
        with conn.cursor() as cr:
            cr.execute(
                f'delete from analysis_results where model="{model_id}" and `scantype`=0')
        r.set('stage', '结果入库')
        r.set('progress', 0)
        # num_new_rel = res_to_db(output, config_map, last_rel_res, logging)
        num_new_rel = res_to_db2(output, conn, last_rel_res, logging)
        num_rel = len(output)
        r.set('progress', 100)

        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sql = f'update analysis_status set analysis_status="2", relation_num={num_rel}, ' \
              f'new_relation_num={num_new_rel},' \
              f'end_time="{end_time}" where id="{model_id}"'
        with conn.cursor() as cr:
            cr.execute(sql)
            conn.commit()
        conn.close()
        r.set('state', 1)
        r.set('msg', 'Complete')
        requests.get(finish_url)
        logging.info(f'计算完成')


def one_db(data_source, logging, custom_para, user_rel_res):
    """对于单系统中的所有的表之间的关系进行查找。

    Args:
        data_source(dict): 源数据库的连接信息
        logging: 日志记录器
        custom_para(tuple): 用户配置的参数
        user_rel_res(list): 用户标记的结果列表

    Returns:

    """
    conn, cr, path, dtype_list, sql1, sql2, sql3, sql4, sql5, \
        sql6, sql7, sql8 = connect(data_source, logging)
    use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len = custom_para
    if not conn:
        return None
    if not data_source['tables']:
        logging.info('未指定表，将对数据库中的所有表进行关系查找')
        cr.execute(sql1)
        ori_tabs = list(map(lambda x: x[0], cr.fetchall()))
    else:
        logging.info('将对指定的表进行关系查找')
        ori_tabs = data_source['tables']

    new_tabs, cols, length, length_long, length_zero, no_pks, \
        pks, no_exist = get_cache_files(path, ori_tabs, logging)

    logging.info('主键与外键查找...')
    new_cols, new_length, new_length_long, new_length_zero, new_no_pks, \
        new_pks, new_no_exist = fine_pk_and_pc(cr, new_tabs, (sql2, sql3, sql4, sql7, sql8),
                                               dtype_list, logging, data_cleansing)
    logging.info('完成')

    r.set('stage', '计算关系2/3')
    logging.info('更新/创建缓存文件...')
    cols.update(new_cols)
    length.update(new_length)
    length_long.update(new_length_long)
    length_zero += new_length_zero
    no_pks += new_no_pks
    pks.update(new_pks)
    no_exist += new_no_exist
    ori_tabs = list(set(ori_tabs) - set(no_exist) - set(length_zero) - set(length_long))
    if not os.path.exists(f'./table_attr/{path}'):
        os.makedirs(f'./table_attr/{path}')
    for v in ['cols', 'length', 'length_long', 'length_zero', 'no_pks', 'pks', 'no_exist']:
        with open(f'./table_attr/{path}/{v}.json', 'w') as f:
            json.dump(eval(v), f)
    logging.info('完成')

    logging.info('创建过滤器...')
    gen_bloom_filter(pks, length, path, conn, logging, sql5)
    logging.info('完成')

    r.set('stage', '计算关系3/3')
    logging.info('计算表关系...')
    results = []
    for i in range(len(ori_tabs)):
        r.set('progress', 100 * i / len(ori_tabs))
        tab = ori_tabs[i]
        logging.info(f'{i + 1:4}/{len(ori_tabs)}:Computing `{tab}`...')
        if tab in not_cite_table_list:  # 不允许有外键连接
            continue
        if length[tab] < inf_tab_len:  # 记录数太少
            continue
        if not cols.get(tab):
            continue
        for col in cols.get(tab):
            try:
                value = pd.read_sql(sql6 % (col, tab), conn, coerce_float=False)
                all_num = value.shape[0]
            except Exception as e:
                logging.exception(e)
                continue
            try:
                df = col_value_filter(value, int(use_str_len), int(inf_str_len), float(inf_dup_ratio))
                if isinstance(df, pd.DataFrame) and df.empty:
                    continue
                if df is None:
                    continue
                for pk_name in pks:
                    if pk_name in not_base_table_list:
                        continue
                    if length[pk_name] < inf_tab_len:
                        continue
                    if pk_name == tab:
                        continue
                    for pk_col in pks[pk_name]:
                        if "".join((path, pk_name, pk_col, path, tab, col)) in user_rel_res:
                            # 用户标记过的数据不进行存储
                            continue
                        with open(f'./filters/{path}/{pk_name}@{pk_col}.filter', 'rb') as f:
                            bf = pickle.load(f)
                        flag = 1
                        num_not_in_bf = 0
                        for k in value[col]:
                            if k not in bf:
                                num_not_in_bf += 1
                            if num_not_in_bf / all_num > sup_out_foreign_key:
                                flag = 0
                                break
                        if flag:  # `flag` equals `0` means these two columns has no relationship.
                            not_match_ratio = num_not_in_bf / all_num
                            res = [data_source['model_id'], path, pk_name, 'table-comment',
                                   pk_col, 'column-comment', data_source['model_id'], path, tab,
                                   'table-comment', col, 'column-comment', not_match_ratio]
                            results.append(res)
            except Exception as e:
                print(e)

    output = pd.DataFrame(columns=['model1', 'db1', 'table1', 'table1comment', 'column1',
                                   'column1comment', 'model2', 'db2', 'table2', 'table2comment',
                                   'column2', 'column2comment', 'matching_degree'],
                          index=range(len(results)))
    if results:
        for i in range(len(results)):
            output.iloc[i] = results[i]
    else:
        return pd.DataFrame()
    return output


def connect(data_source, logging):
    """根据`data_source`, 得到数据库的连接对象以及一些有用的SQL。

    sql1：获取当前数据库/用户下的所有表的名称
    sql2：获取指定表中的所有字段及其数据类型
    sql3：获取指定表的指定字段的非空数据的个数
    sql4：获取指定表的指定字段的去重后的数据的个数
    sql5：获取指定表的指定字段的全部内容
    sql6：获取指定表的指定字段的1000行内容
    sql7：获取指定表的总行数
    sql8：获取某字段去除中文后的数据的个数

    目前，支持四种数据库类型: MySQL, Gbase, Oracle, PostgreSQL.

    Args:
        data_source(dict): 参数字典
        logging: 日志记录器

    Returns:

    """
    # MySQL与Gbase 均可以通过pymysql进行连接
    if data_source['type'].upper() in ('MYSQL', 'GBASE'):
        logging.info(f'发现MySQL/Gbase数据源')
        config = data_source['config']
        mysql_config = dict((key, value) for key, value in config.items()
                            if key in ['db', 'host', 'password', 'port', 'user'])
        conn = pymysql.connect(**mysql_config, charset='utf8')
        cr = conn.cursor()
        # A column will be calculated only when its data type in `dtype_list`
        dtype_list = mysql_type_list
        db = config['db']
        sql1 = f"select `table_name` from information_schema.tables where table_schema='{db}' " \
               f"and table_type='BASE TABLE'"
        sql2 = f"select column_name, data_type from information_schema.columns where " \
               f"table_schema='{db}' " \
               f"and table_name='%s'"
        sql3 = f'select count(`%s`) from {db}.`%s`'
        sql4 = f'select count(distinct `%s`) from {db}.%s'
        sql5 = f'select `%s` from {db}.`%s`'
        sql6 = f'select `%s` from {db}.`%s` limit 1000'
        sql7 = f'select count(1) from {db}.`%s`'
        sql8 = f'select count(*) from {db}.`%s` where length(`%s`)=char_length(`%s`)'
    elif data_source['type'].upper() == 'ORACLE':
        logging.info('发现Oracle数据源')
        import cx_Oracle
        config = data_source['config']
        db = config['db']
        user = config['user']
        url = config['url']
        url = url.split('@')[1]
        conn = cx_Oracle.connect(user, config['password'], url)
        cr = conn.cursor()
        multi_schema = config.get('multi_schema')
        target_schema = config.get('target_schema')
        # A column will be calculated only when its data type in `dtype_list`
        dtype_list = oracle_type_list
        if multi_schema:
            logging.info("多模式")
            db = target_schema
        else:
            logging.info("单模式")
        sql1 = f"select table_name from all_tables where owner='{db}'"
        sql2 = f"select column_name, data_type from all_tab_columns where table_name='%s' " \
               f"and owner='{db}'"
        sql3 = f'select count("%s") from {db}."%s"'
        sql4 = f'select count(distinct "%s") from {db}."%s"'
        sql5 = f'select "%s" from {db}."%s"'
        sql6 = f'select "%s" from {db}."%s" where rownum <= 1000'
        sql7 = f'select count(1) from {db}."%s"'
        sql8 = f'select count(1) from {db}."%s" where length("%s") = lengthb("%s")'
    elif data_source['type'].upper() == 'POSTGRESQL':
        logging.info('发现PostgreSQL数据源')
        import psycopg2
        config = data_source['config']
        # A column will be calculated only when its data type in `dtype_list`
        dtype_list = pg_type_list
        conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'],
                                password=config['password'], database=config['db'])
        db = config['db']
        cr = conn.cursor()
        pattern = config['pattern']
        sql1 = f"select tablename from pg_tables where schemaname='{pattern}'"
        sql2 = f'select a.attname as name, substring(format_type(a.atttypid, a.atttypmod) from ' \
               f'"[a-zA-Z]*") as ' \
               f'type from pg_class as c, pg_attribute as a, pg_namespace as p where ' \
               f'c.relnamespace=p.oid ' \
               f'and c.relname="%s" and a.attrelid=c.oid and a.attnum > 0 and p.nspname="{pattern}"'
        sql3 = f'select count("%s") from {pattern}.%s'
        sql4 = f'select count(distinct "%s") from {pattern}.%s'
        sql5 = f'select "%s" from {pattern}.%s'
        sql6 = f'select "%s" from {pattern}.%s '
        sql7 = f'select count(1) from {pattern}.%s'
        sql8 = f'select count(*) from {pattern}.%s where length("%s") = ("%s")'
    else:
        logging.error('Data source type is invalid.')
        db = conn = cr = dtype_list = sql1 = sql2 = sql3 = sql4 = sql5 = sql6 = sql7 = sql8 = None
    return conn, cr, db, dtype_list, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8


def get_cache_files(path, ori_tabs, logging):
    """尝试获取缓存文件以加速计算。

    Args:
        path(str): 缓存文件路径
        ori_tabs(list): 表名组成的列表
        logging: 日志记录器

    Returns:

    """
    logging.info('尝试获取缓存文件...')
    try:
        # 不使用缓存
        with open('./table_attr/unexistfile') as f:
            _ = json.load(f)
        #
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
        # `new_tabs` are these tables that needed to generate filters for some fields.
        new_tabs = list(set(ori_tabs) - set(cached_length) - set(cached_length_long) - set(cached_length_zero)
                        - set(cached_no_exist))
        cols = cached_cols.copy()
        length = cached_length.copy()
        length_long = cached_length_long.copy()
        length_zero = cached_length_zero.copy()
        no_pks = cached_no_pks.copy()
        pks = cached_pks.copy()
        no_exist = cached_no_exist.copy()
        logging.info('成功')
    except IOError:
        logging.info('无可用的缓存文件')
        new_tabs = ori_tabs
        cols = {}
        length = {}
        length_long = {}
        length_zero = []
        no_pks = []
        pks = {}
        no_exist = []
    return new_tabs, cols, length, length_long, length_zero, no_pks, pks, no_exist


def fine_pk_and_pc(cr, tabs, sqls, dtype_list, logging, data_cleansing):
    """查找表的主键与外键。

    Args:
        cr: 数据库连接的游标对象
        tabs(list): 表名列表
        sqls(tuple): sql语句组成的元组
        dtype_list(list): 数据类型组成的列表
        logging: 日志记录器
        data_cleansing(dict): 数据过滤规则字典

    Returns:

    """
    sql2, sql3, sql4, sql7, sql8 = sqls
    cols = {}
    length = {}
    length_long = {}
    length_zero = []
    no_pks = []
    pks = {}
    no_exist = []
    start_time_first = time.perf_counter()
    for i in range(len(tabs)):
        start_time = time.perf_counter()
        tab = tabs[i]
        logging.info(f'{i + 1}/{len(tabs)}: `{tab}` starting...')
        r.set('progress', 100 * (i + 1) / len(tabs))
        try:
            cr.execute(sql7 % tab)
        except Exception as e:
            no_exist.append(tab)
            logging.info(f'{" " * 6}`{tab}` does not exist:{e}!')
            continue
        row_num = cr.fetchone()[0]
        if row_num > 1e8:
            logging.info(f'{" " * 6}`{tab}` is too long to be skipped')
            length_long[tab] = row_num
            continue
        elif row_num == 0:
            logging.info(f'{" " * 6}`{tab}` has no data and is skipped')
            length_zero.append(tab)
            continue
        length[tab] = row_num
        try:
            cr.execute(sql2 % tab)
        except Exception as e:
            logging.error(e)
            continue
        cols_dtype = cr.fetchall()
        pos_pks = []
        pos_cols = []
        for j in range(len(cols_dtype)):
            col_name = cols_dtype[j][0]
            col_type = cols_dtype[j][1]
            if col_type.upper() not in dtype_list:
                continue
            if not col_name_filter(tab, col_name, data_cleansing):
                continue
            try:
                cr.execute(sql3 % (col_name, tab))
            except Exception as e:
                logging.error(e)
                logging.error(sql3 % (col_name, tab))
                continue
            num1 = cr.fetchone()[0]  # 某个字段的非空数据总数
            if num1 == row_num:
                try:
                    cr.execute(sql4 % (col_name, tab))
                except Exception as e:
                    logging.error(e)
                    logging.error(sql4 % (col_name, tab))
                    continue
                num2 = cr.fetchone()[0]  # 某个字段的去重后的非空数据总数
                try:
                    cr.execute(sql8 % (tab, col_name, col_name))
                except Exception as e:
                    logging.error(e)
                    logging.error(sql8 % (tab, col_name, col_name))
                    continue
                num3 = cr.fetchone()[0]  # 某个字段去除中文字符后的非空数据总数
                if num1 == num2 and num1 == num3:  # 数据无重复且无中文
                    pos_pks.append(col_name)
                    if both_roles:
                        pos_cols.append(col_name)
                    elif num1 == num3:
                        pos_cols.append(col_name)
                else:
                    pos_cols.append(col_name)
        if len(pos_pks):
            pks[tab] = pos_pks
            logging.info(f'{" " * 6}Num of possible primary keys of table `{tab}`:{len(pos_pks)}')
        else:
            no_pks.append(tab)
            logging.info(f'{" " * 6}`{tab}` has no possible primary keys.')
        cols[tab] = pos_cols
        if len(pos_cols):
            logging.info(f'{" " * 6}Num of possible foreign keys of table `{tab}`:{len(pos_cols)}')
        else:
            logging.info(f'{" " * 6}`{tab}` has no possible foreign keys.')
        run_time = time.perf_counter() - start_time
        logging.info(f"{' ' * 6}`{tab}`'s info: \t# records:{row_num}\t # fields:{len(cols_dtype)}\t "
                     f"run time:{run_time:.3f}")
    end_time_last = time.perf_counter()
    logging.info(f"""
    主外键查找统计：
        有效表总数：{len(length)}
        空表总数  ：{len(length_zero)}
        超大表总数：{len(length_long)}
        有效记录数：{sum(length.values())}
        耗时     ：{end_time_last - start_time_first}
    """)
    return cols, length, length_long, length_zero, no_pks, pks, no_exist


def gen_bloom_filter(pks, length, path, conn, logging, sql5):
    """为主键字段生成过滤器文件。

    Args:
        pks(dict): 存储表名及其可能主键的字典
        length(dict): 表名及其长度的字典
        path(str): 存储路径
        conn: 数据库连接对象
        logging: 日志记录器
        sql5(str): sql语句

    Returns:

    """
    if not os.path.exists(f'./filters/{path}/'):
        os.makedirs(f'./filters/{path}/')
    logging.info('Try to get existed filter files...')
    try:
        with open(f'./filters/{path}/filters.json') as f:
            filters = json.load(f)
        logging.info('Success')
    except Exception as e:
        logging.info('Cannot get filter files')
        logging.info(e)
        filters = {}

    total_num = sum(list(map(lambda x: len(pks[x]), list(pks))))
    logging.info(f'{total_num} filters will be created at most.')
    n = 1
    for i in range(len(pks)):
        tab = list(pks.keys())[i]
        capacity = length[tab] * 2  # The capacity of a filter file.
        cols = pks[tab]
        for k in range(len(cols)):
            r.set('progress', 100 * n / total_num)
            t_s = time.time()
            col = cols[k]
            logging.info(f'{n:4}/{total_num:4}:Computing {tab}.{col}, {length[tab]} rows in total.')
            if tab + '@' + col in filters:
                logging.info(f'{" " * 9}{tab}.{col} already exists, continue.')
                n += 1
                continue
            if os.path.exists(f'./filters/{path}/{tab}@{col}.filter'):
                logging.info(f'{" " * 9}{tab}.{col} already exists, continue.')
                n += 1
                continue
            value = pd.read_sql(sql5 % (col, tab), conn, coerce_float=False)
            if value.shape[0] > capacity:
                capacity = value.shape[0] * 2
            if multi_process and length[tab] > 1e6:
                bf = add_operation(value, capacity)
            else:
                bf = BloomFilter(capacity)
                for j in value.iloc[:, 0]:
                    bf.add(j)
            with open(f'./filters/{path}/{tab}@{col}.filter', 'wb') as f:
                pickle.dump(bf, f)
            filters[tab + '@' + col] = capacity
            t_e = time.time()
            logging.info(f'{" " * 9}{tab}.{col} finished, cost {t_e - t_s:.2f}s')
            n += 1
    with open(f'./filters/{path}/filters.json', 'w') as f:
        json.dump(filters, f)

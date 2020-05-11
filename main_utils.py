# -*- coding: utf-8 -*-
"""
@Time       : 2020/2/10 14:13
@Author     : Jarvis
@Annotation : Sorry for this shit code
"""
from utils import check_connection, check_parameters, gen_logger
from utils import col_name_filter, col_value_filter, res_to_db, roll_back
from pymysql.connections import Connection
import pymysql
import time
import json
import os
import requests
import redis
import pickle
import pandas as pd
from pybloom import BloomFilter
from config import both_roles, not_cite_table, not_base_table, sup_out_foreign_key, \
    multi_process, redis_config, finish_url
if multi_process:
    from faster import add_operation

not_base_table_list = [] if not not_base_table else list(not_base_table.split(','))
not_cite_table_list = [] if not not_cite_table else list(not_cite_table.split(','))


class R:
    def get(self, name):
        pass

    def set(self, name, value):
        pass


try:
    r = redis.Redis(**redis_config)
    r.ping()
except Exception as err:
    print(err)
    print('Can not connect to redis and progress is not available.')
    r = R()


def main_process(post_json):
    """The main function of calculation.

    Args:
        post_json(dict): All json-format parameters posted by user.

    Returns:

    """
    global r
    r.set('stage', '初始化')  # Stage of calculation
    r.set('progress', 0)  # Progress, value between 0 and 100
    r.set('msg', '')  # Additional message
    r.set('state', 0)  # Calculation state, 0 means calculating, 1 means finished(or some errors occur)
    # Check if the parameters is valid.
    if isinstance(check_parameters(post_json), str):
        r.set('progress', 0)
        r.set('state', 1)
        r.set('msg', check_parameters(post_json))
        return {'state': 0, 'msg': check_parameters(post_json)}
    # Check if the config database is connectable.
    config_map = post_json['configMap']
    conn = check_connection(config_map)
    if not isinstance(conn, Connection):
        msg = 'A connection error occurs when connecting to MySQL database. See console for more information.'
        r.set('progress', 10)
        r.set('state', 1)
        r.set('msg', msg)
        return {'state': 0, 'msg': msg}
    # Get all parameters and initialize a log object.
    model_id1 = post_json['modelId1']
    model_id2 = post_json['modelId2']
    model_id = (model_id1 + model_id2) if model_id1 < model_id2 else (model_id2 + model_id1)
    alg_name = post_json['algorithmName']
    exe_obj = post_json['executObj']
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logging = gen_logger(model_id1+'-'+model_id2)
    logging.info(f'{"*"*80}')
    logging.info('All parameters required are satisfied and starting calculation'.upper())
    logging.info(f'{"*"*80}')
    logging.info(f'model 1: {model_id1}')
    logging.info(f'model 2: {model_id2}')
    if multi_process:
        logging.warning('Multi process mode')
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Initialize the status table.
    # Note:
    #   - For a new model, which means there are no relation results before, program will create
    #   a new record in status table with '1'(means analyzing) in `analysisstatus` field and
    #   rewrite it to '2'(means analyzed). If some errors occur, program will delete this new
    #   record.
    #   - For a model that already has relation results, program will re-calculate and overwrite
    #   its results(some results may be kept, the keeping rules is defined in following code). To
    #   do this, program will find the model status record(by `model_id`) first and change its
    #   `analysisstatus` to '1'. After calculation, set it to '2'. If some errors occur, program
    #   will rollback the value to '2'.
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    logging.info('Initializing the analysis status table...')
    r.set('progress', 30)

    sql = f'REPLACE INTO pf_analysis_status (linkageid, analysisstatus, algorithmname, executobj,' \
          f'start_time) values("{model_id}", "1", "{alg_name}", "{exe_obj}", "{start_time}")'
    try:
        with conn.cursor() as cr:
            # Cache the status of model for rollback when needed
            cr.execute(f'select t.analysisstatus from analysis_status t where t.linkageid={model_id}')
            res = cr.fetchone()
            if not res:
                status_bak = False
                logging.info('This is a new model, rollback will delete status record.')
            else:
                status_bak = res[0]
                logging.info('This is a re-calculation model, rollback will set `status` value to original.')
            # Change the status to 'analyzing'
            cr.execute(sql)
            conn.commit()
            r.set('progress', 100)
    except Exception as e:
        logging.error(e)
        logging.info('Starting roll back...')
        try:
            roll_back(status_bak, conn, model_id)
            logging.info('Roll back success.')
        except Exception as e:
            # This error is due to the undefined of `status_bak`.
            logging.info('An error occurs when rolling back.')
            logging.error(e)
        r.set('progress', 0)
        r.set('state', 1)
        return {'state': 0, 'msg': 'An error occurs when initializing status table.'}
    logging.info('Initialization complete.')
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #   `custom parameters` are critical values for restraint conditions. Since the accuracy of relation results is
    # deeply affected by the data source tables, such as table's length, fields' data type and value length, etc,
    # it is very useful to let user specify the critical values so that the result accuracy will be improved.
    #   Users can edit parameters on front page and the parameters will be stored into config database. Parameters are
    # distinguished by `model_id`, which means you can only change parameters of one model at a time. If users don't
    # want to change them, the defaults will be used.
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    r.set('stage', '参数获取')  # Stage of calculation
    r.set('progress', 0)  # Progress, value between 0 and 100
    r.set('msg', '')  # Additional message
    logging.info('Getting custom parameters from user config table...')
    sql = f'select t.csl, t.ds, t.idr, t.isl, t.itl, t.tables1, t.tables2 from ' \
          f'pf_user_config t where t.model="{model_id}"'
    try:
        with conn.cursor() as cr:
            if cr.execute(sql):
                logging.info('Get custom parameters successfully.')
                res = cr.fetchone()
                use_str_len = str(res[0]) if res[0] else '0'
                data_cleansing = eval(str(res[1])) if res[1] else {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = res[2] if res[2] else 0.4
                inf_str_len = res[3] if res[3] else 3
                inf_tab_len = res[4] if res[4] else 10
                tables1 = list(res[5].split(',')) if res[5] else []
                tables2 = list(res[6].split(',')) if res[6] else []
            else:
                logging.warning('No custom parameters and defaults will be used.')
                use_str_len = '0'
                data_cleansing = {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = 0.4
                inf_str_len = 3
                inf_tab_len = 10
                tables1 = []
                tables2 = []
            # Merge custom parameters to a tuple
            custom_para = (use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len)
            r.set('progress', 100)
    except Exception as e:
        logging.error(e)
        logging.info('Staring roll back...')
        roll_back(status_bak, conn, model_id)
        logging.info('Roll back success.')
        r.set('state', 1)
        r.set('msg', 'An error occurs when getting custom parameters.')
        return {'state': 0, 'msg': 'An error occurs when getting custom parameters.'}
    logging.info('Getting custom parameters complete.')
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #   If re-compute a model, program will delete old results firstly. The deletion is valid only for non-tagged
    # results.
    #   A tagged result is a record that was added/edited/deleted by users. For normal records, program will mark '0' to
    # `scantype` field; For added/edited records, program will mark '1' to `scantype` field.
    #
    last_rel_res = []  # cache the last computation results
    user_rel_res = []  # cache the user tagged results
    if status_bak:  # a re-compute model
        r.set('stage', '删除旧结果')
        r.set('progress', 0)
        r.set('msg', '')
        try:
            logging.info('Caching last computation results and deleting non-tagged results...')
            with conn.cursor() as cr:
                cr.execute(f'select db1, table1, column1, db2, table2, column2 from pf_analysis_result1 where '
                           f'model="{model_id}"')
                res = cr.fetchall()
                for r_ in res:
                    last_rel_res.append("".join(r_))
                cr.execute(f'delete from pf_analysis_result1 where model="{model_id}" and `scantype`="0"')
                cr.execute(f'select db1, table1, column1, db2, table2, column2 from pf_analysis_result1 where'
                           f'model="{model_id}" and `scantype` != "0"')
                res = cr.fetchall()
                for r_ in res:
                    user_rel_res.append("".join(r_))
            conn.commit()
            r.set('progress', 100)
        except Exception as e:
            logging.error(e)
            logging.info('Starting roll back...')
            roll_back(status_bak, conn, model_id)
            logging.info('Roll back success.')
            r.set('state', 1)
            r.set('msg', 'An error occurs when caching old results.')
            return {'state': 0, 'msg': 'An error occurs when caching old results.'}
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #   After initialization, program will get data source information through model id and connect data source
    # to get data. The source tables can come from both one single database or two different databases.
    #
    if model_id1 == model_id2:  # In this branch, all tables come from one single database.
        r.set('stage', '计算关系1/3')
        r.set('progress', 0)
        logging.info('All tables come from one single database.')
        with conn.cursor() as cr:
            cr.execute(f'select datasource_id from pf_businessmodel_model t where t.id="{model_id1}"')
            ds = cr.fetchone()[0]
            data_source = {}
            cr.execute(f'select type, host, port, user_name, password, sid, schema_name, from pf_datasource t where '
                       f't.id="{ds}"')
            res = cr.fetchone()
            data_source['model_id'] = model_id1
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
            output = one_db(data_source, logging, custom_para, user_rel_res)
            if not output:
                r.set('state', 1)
                r.set('msg', 'Empty output')
                logging.info('Empty output')
                roll_back(status_bak, conn, model_id)
                return
            r.set('stage', '结果入库')
            r.set('progress', 0)
            num_new_rel = res_to_db(output, config_map, last_rel_res, logging)
            num_rel = len(output)
            r.set('progress', 100)

    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    sql = f'update pf_analysis_status set analysisstatus="2", relationnum={num_rel}, new_relation_num={num_new_rel},' \
          f'end_time="{end_time}" where linkageid="{model_id}"'
    with conn.cursor() as cr:
        cr.execute(sql)
        conn.commit()
    conn.close()
    r.set('state', 1)
    r.set('msg', 'Complete')
    logging.info(f'calculation complete.')
    requests.get(finish_url)


def one_db(data_source, logging, custom_para, user_rel_res):
    """This function is used to handle that all tables come from the same database.

    Args:
        data_source(dict): A dict contains all information for connecting data source.
        logging: A log object.
        custom_para(tuple): A tuple contains custom parameters.
        user_rel_res(list): A list contains relation results that modified by user.

    Returns:

    """
    conn, cr, path, dtype_list, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8 = connect(data_source, logging)
    use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len = custom_para
    if not conn:
        return None
    if not data_source['tables']:
        logging.info('Not specify table names, all tables in database will be used.')
        cr.execute(sql1)
        ori_tabs = list(map(lambda x: x[0], cr.fetchall()))
    else:
        logging.info('The specified tables will be used.')
        ori_tabs = data_source['tables']

    new_tabs, cols, length, length_long, length_zero, no_pks, pks, no_exist = get_cache_files(path, ori_tabs, logging)

    logging.info('Finding primary keys and possible foreign keys...')
    new_cols, new_length, new_length_long, new_length_zero, new_no_pks, \
        new_pks, new_no_exist = fine_pk_and_pc(cr, new_tabs, (sql2, sql3, sql4, sql7, sql8),
                                               dtype_list, logging, data_cleansing)
    logging.info('Finished.')

    r.set('stage', '计算关系2/3')
    logging.info('Update/Create cache files...')
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
    logging.info('Finished.')

    logging.info('Generating filter-files for new tables...')
    gen_bloom_filter(pks, length, path, conn, logging, sql5)
    logging.info('Finished.')

    r.set('stage', '计算关系3/3')
    logging.info('Computing table relations...')
    results = []
    for i in range(len(ori_tabs)):
        r.set('progress', 100*i/len(ori_tabs))
        tab = ori_tabs[i]
        logging.info(f'{i+1:4}/{len(ori_tabs)}:Computing `{tab}`...')
        if tab in not_cite_table_list:  # Not allowed to have foreign keys
            continue
        if length[tab] < inf_tab_len:  # Table is too long
            continue
        for col in cols[tab]:
            try:
                value = pd.read_sql(sql6 % (tab, col), conn, coerce_float=False)
                all_num = value.shape[0]
            except Exception as e:
                logging.exception(e)
                continue
            if not col_value_filter(value, int(use_str_len), int(inf_str_len), float(inf_dup_ratio)):
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
                        # skip the results edited by users.
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
                        res = [data_source['model_id'], path, pk_name, 'table-comment', pk_col, 'column-comment',
                               data_source['model_id'], path, tab, 'table-comment', col, 'column-comment',
                               not_match_ratio]
                        results.append(res)
    output = pd.DataFrame(columns=['model1', 'db1', 'table1', 'table1comment', 'column1', 'column1comment',
                                   'model2', 'db2', 'table2', 'table2comment', 'column2', 'column2comment',
                                   'matching_degree'],
                          index=range(len(results)))
    for i in range(len(results)):
        output.iloc[i] = results[i]
    return output


def connect(data_source, logging):
    """According to `data_source`, this function try to connect database and return some useful object for later
    calculation.
        This function supports four kinds of database: MySQL, Gbase, Oracle, PostgreSQL.

    Args:
        data_source(dict): A dict contains all information for connecting data source.
        logging: Log object.

    Returns:
        Connection objects and some useful sql.
    """
    # MySQL and Gbase databases share the same module.
    if data_source['db_type'].upper() in ('MYSQL', 'GBASE'):
        logging.info(f'A MySQL/Gbase data source found.')
        config = data_source['config']
        if 'pattern' in config.keys():
            config.pop('pattern')
        conn = pymysql.connect(**config, charset='utf8')
        cr = conn.cursor()
        # A column will be calculated only when its data type in `dtype_list`
        dtype_list = ['VARCHAR', 'DECIMAL', 'CHAR', 'TEXT']
        db = config['db']
        sql1 = f'select `table_name` from information_schema.tables where table_schema="{db}" ' \
               f'and table_type="BASE TABLE"'
        sql2 = f'select column_name, data_type from information_schema.columns where table_schema="{db}" ' \
               f'and table_name="%s"'
        sql3 = f'select count(`%s`) from "{db}".`%s`'
        sql4 = f'select count(distinct `%s`) from {db}.%s'
        sql5 = f'select `%s` from {db}.`%s`'
        sql6 = f'select `%s` from {db}.`%s` limit 1000'
        sql7 = f'select count(1) from {db}.`%s`'
        sql8 = f'select count(*) from {db}.`%s` where length(`%s`)=char_length(`%s`)'
    elif data_source['db_type'].upper() == 'ORACLE':
        logging.info('An Oracle data source found.')
        import cx_Oracle
        config = data_source['config']
        url = config['host'] + ':' + str(config['port']) + '/' + config['db']
        conn = cx_Oracle.connect(config['user'], config['password'], url)
        cr = conn.cursor()
        # A column will be calculated only when its data type in `dtype_list`
        dtype_list = ['VARCHAR2', 'CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR2']
        db = config['user']
        pattern = config['pattern']
        if len(pattern) != 0:
            db = pattern
        sql1 = f'select table_name from all_tables where owner="{db}"'
        sql2 = f'select column_name, data_type from all_tab_columns where table_name="%s" and owner="{db}"'
        sql3 = f'select count("%s") from {db}."%s"'
        sql4 = f'select count(distinct "%s") from {db}."%s"'
        sql5 = f'select "%s" from {db}."%s"'
        sql6 = f'select "%s" from {db}."%s" where rownum <= 1000'
        sql7 = f'select count(1) from {db}."%s"'
        sql8 = f'select count(1) from {db}."%s" where length("%s") = lengthb("%s")'
    elif data_source['db_type'].upper() == 'POSTGRESQL':
        logging.info('A PostgreSQL data source found.')
        import psycopg2
        config = data_source['config']
        # A column will be calculated only when its data type in `dtype_list`
        dtype_list = ['CHARACTER', 'TEXT']
        conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'],
                                password=config['password'], database=config['db'])
        db = config['db']
        cr = conn.cursor()
        pattern = config['pattern']
        sql1 = f"select tablename from pg_tables where schemaname='{pattern}'"
        sql2 = f'select a.attname as name, substring(format_type(a.atttypid, a.atttypmod) from "[a-zA-Z]*") as ' \
               f'type from pg_class as c, pg_attribute as a, pg_namespace as p where c.relnamespace=p.oid ' \
               f'and c.relname="%s" and a.attrelid=c.oid and a.attnum > 0 and p.nspname="{pattern}"'
        sql3 = f'select count("%s") from {pattern}.%s'
        sql4 = f'select count(distinct "%s") from {pattern}.%s'
        sql5 = f'select "%s" from {pattern}.%s'
        sql6 = f'select "%s" from {pattern}.%s '
        sql7 = f'select count(1) from {pattern}.%s'
        sql8 = f'select count(*) from {pattern}.%s where length("%s") = octet_length("%s")'
    else:
        logging.error('Data source type is invalid.')
        db = conn = cr = dtype_list = sql1 = sql2 = sql3 = sql4 = sql5 = sql6 = sql7 = sql8 = None
    return conn, cr, db, dtype_list, sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8


def get_cache_files(path, ori_tabs, logging):
    """Try to get the cached filter files to speed calculation.

    Args:
        path(str): Subdirectory to filters.
        ori_tabs(list): List of original table names.
        logging: Log object.

    Returns:
        Cached information and new tables to generate filters.
    """
    logging.info('Trying to get cached filter files...')
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
        logging.info('Getting caches success.')
    except IOError:
        logging.info('No caches available.')
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
    """Find the primary keys and possible foreign keys.

    Args:
        cr: A connected database cursor object.
        tabs(list): List of table names that needed to be calculated.
        sqls(tuple): A tuple of SQL statements:
            sql2-SQL statement used to find column name and its data type.
            sql3-SQL statement used to find length of a column.
            sql4-SQL statement used to find length of a distincted column.
            sql7-SQL statement used to find length of a table.
            sql8-SQL statement used to find ...
        dtype_list(list): A list of data types.
        logging: Log object.
        data_cleansing(dict): A dict contains filter rules.

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
    for i in range(len(tabs)):
        start_time = time.clock()
        tab = tabs[i]
        logging.info(f'{i+1}/{len(tabs)}: `{tab}` starting...')
        r.set('progress', 100*(i+1)/len(tabs))
        try:
            cr.execute(sql7 % tab)
        except Exception as e:
            no_exist.append(tab)
            logging.info(f'`{tab}` does not exist:{e}!')
            continue
        row_num = cr.fetchone()[0]
        if row_num > 1e8:
            logging.info(f'`{tab}` is too long to be skipped')
            length_long[tab] = row_num
            continue
        elif row_num == 0:
            logging.info(f'`{tab}` has no data and is skipped')
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
                if num1 == num2 and num1 == num3:
                    pos_pks.append(col_name)
                    if both_roles:
                        pos_cols.append(col_name)
                    elif num1 == num3:
                        pos_cols.append(col_name)
                else:
                    pos_cols.append(col_name)
            if len(pos_pks):
                pks[tab] = pos_pks
                logging.info(f'{" " * 6} # of possible primary keys of table `{tab}`:{len(pos_pks)}')
            else:
                no_pks.append(tab)
                logging.info(f'{" " * 6} `{tab}` has no possible primary keys.')
            cols[tab] = pos_cols
            if len(pos_cols):
                logging.info(f'{" " * 6} # of possible foreign keys of table `{tab}`:{len(pos_cols)}')
            else:
                logging.info(f'{" " * 6} `{tab}` has no possible foreign keys.')
            run_time = time.clock() - start_time
            logging.info(f"`{tab}`'s info: \t# records:{row_num}\t # fields:{len(cols_dtype)}\t "
                         f"run time:{run_time:.3f}")
    return cols, length, length_long, length_zero, no_pks, pks, no_exist


def gen_bloom_filter(pks, length, path, conn, logging, sql5):
    """Generate bloom-filter file for primary keys of `pks`.

    Args:
        pks(dict): Table names and their possible primary keys.
        length(dict): Table names and their table length.
        path(str): Directory to store filter files.
        conn: A database connection object.
        logging: Log object.
        sql5(str): SQL statement used to ...

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
    logging.info(f'{total_num} filters will be created as most.')
    n = 1
    for i in range(len(pks)):
        tab = list(pks.keys())[i]
        capacity = length[tab] * 2  # The capacity of a filter file.
        cols = pks[tab]
        for k in range(len(cols)):
            r.set('progress', 100*n/total_num)
            t_s = time.time()
            col = cols[k]
            logging.info(f'{n:4}/{total_num:4}:Computing {tab}.{col}, {length[tab]} rows in total.')
            if tab + '@' + col in filters:
                logging.info(f'{tab}.{col} already exists, continue.')
                n += 1
                continue
            value = pd.read_sql(sql5 % (col, tab), conn, coerce_float=False)
            if multi_process:
                bf = add_operation(value, capacity)
            else:
                bf = BloomFilter(capacity)
                for j in value.iloc[:, 0]:
                    bf.add(j)
            with open(f'./filters/{path}/{tab}@{col}.filter', 'wb') as f:
                pickle.dump(bf, f)
            filters[tab + '@' + col] = capacity
            t_e = time.time()
            logging.info(f'{tab}.{col} finished, cost {t_e-t_s:.2f}s')
            n += 1
    with open(f'./filters/{path}/filters.json', 'w') as f:
        json.dump(filters, f)

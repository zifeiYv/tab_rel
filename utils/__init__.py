# -*- coding: utf-8 -*-
# todo: 1、增加支持hive的逻辑
#       2、仔细核查利用时间戳确定表是否更新的机制与缓存机制
#       3、添加从源码安装的方式
#
from .utils import check_connection, roll_back, res_to_db2, get_cache_files, col_value_filter, col_name_filter
import logging
import time
from config import multi_process, mysql_type_list, oracle_type_list, sup_out_foreign_key, use_cache, \
    not_base_table, not_cite_table, both_roles
import traceback
import pymysql
import cx_Oracle
import os
import json
import pickle
from pybloom import BloomFilter
import pandas as pd
import psycopg2
from faster import add_operation

not_base_table_list = [] if not not_base_table else list(not_base_table.split(','))
not_cite_table_list = [] if not not_cite_table else list(not_cite_table.split(','))


def main(**kwargs):
    """计算关系的主函数"""
    model_id = kwargs['model_id']
    notify_url = kwargs['notify_url']
    execute_obj = kwargs['execute_obj']
    alg_name = kwargs['alg_name']
    logger = logging.getLogger(model_id)

    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cfg = {
        'user': kwargs['cfg_user'],
        'host': kwargs['cfg_host'],
        'port': int(kwargs['cfg_port']),
        'passwd': kwargs['cfg_passwd'],
        'db': kwargs['cfg_db']
    }
    conn, err_msg = check_connection(cfg)
    if err_msg:
        logger.error(conn)
        return {'state': 0, 'msg': conn}

    if multi_process:
        logger.info('开启多进程模式')
    else:
        logger.info('开启单进程模式')

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Step 1 初始化模型的状态表。
    # 注意:
    #   - 对于一个新建的模型，结果表中是不存在旧结果的，因此，程序会在状态表中插入一条新记录，字段
    #   "analysisstatus"的值为1（代表"计算中"），并且在计算完成后将这个值改写为2（代表"计算完成"）。
    #   如果在计算过程中出现来一些错误，那么程序会删除这一条新增的记录。
    #   - 对于一个已经计算过关系的模型，结果表中可能存在旧结果，因此，重新计算时，程序会将旧结果全部删除（用户
    #   指定的某些结果会得到保留）。另外，由于已经计算过，所以状态表中字段"analysisstatus"的值为2，程序首先
    #   将其改写为1，如果计算完成，再改写为1。如果过程中出错，那么将把该值重新恢复成2。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    logger.info('初始化状态表...')
    sql = f'REPLACE INTO analysis_status (id, analysis_status, algorithm_name, execute_obj,' \
          f'start_time) values("{model_id}", "1", "{alg_name}", "{execute_obj}", "{start_time}")'
    try:
        with conn.cursor() as cr:
            # Cache the status of model for rollback when needed
            cr.execute(f"select t.analysis_status from analysis_status t where t.id='{model_id}'")
            res = cr.fetchone()
            if not res:
                status_bak = False
                logger.info('模型首次参与运行，回滚操作时将会删除状态表中的记录。')
            else:
                status_bak = res[0]
                logger.info('模型已经存在运算结果，回滚操作将会把状态值改写为2。')
            # Change the status to 'analyzing'
            cr.execute(sql)
            conn.commit()
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        logger.info('开始回滚...')
        try:
            roll_back(status_bak, conn, model_id)
            logger.info('回滚完成')
        except Exception as e:
            # This error is due to the undefined of `status_bak`.
            logger.info('回滚出错')
            logger.error(e)
        return {'state': 0, 'msg': '初始化状态表时出错'}
    logger.info('初始化完成')

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Step 2 获取用户参数
    #   "custom parameters"记录了一些临界值，这些临界值对于计算结果的准确性是有非常大的影响的。用户在充分理解
    #   各个值的作用的情况下，调整这些值有利于使结果更加合理。
    #   如果用户不加修改，那么会采用默认值。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    logger.info('获取用户参数...')
    sql = f'select t.csl, t.ds, t.idr, t.isl, t.itl, t.tables1, t.tables2 from ' \
          f'pf_user_config t where t.model="{model_id}{model_id}"'
    try:
        with conn.cursor() as cr:
            if cr.execute(sql):
                logger.info('获取用户参数成功。')
                res = cr.fetchone()
                use_str_len = str(res[0]) if res[0] else '0'
                data_cleansing = eval(str(res[1])) if res[1] else {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = res[2] if res[2] else 0.4
                inf_str_len = res[3] if res[3] else 3
                inf_tab_len = res[4] if res[4] else 10
                tables1 = list(res[5].split(',')) if res[5] else []
            else:
                logger.warning('当前未指定参数，将采用默认值。')
                use_str_len = '0'
                data_cleansing = {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = 0.4
                inf_str_len = 3
                inf_tab_len = 10
                tables1 = []
            # Merge custom parameters to a tuple
            custom_para = (use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len)
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        logger.info('开始回滚...')
        roll_back(status_bak, conn, model_id)
        logger.info('回滚成功')
        return {'state': 0, 'msg': '获取参数过程出错'}
    logger.info('完成参数获取')

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Step 3 开始计算
    #   上述步骤完成后，开始读取数据进行计算。
    #   如果模型并非首次进行运算，那么在写入新的结果前会把部分旧的结果删除。
    #   如果旧的结果是用户手动添加的或者经过了用户的编辑或是用户删除的数据，那么在结果表中的"scantype"字段
    #   会以"1"标示；否则，以"0"标示。在执行删除操作时，只会删除"scantype"取值为"0"的记录。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    last_rel_res = []  # 缓存上次计算的结果
    user_rel_res = []  # 缓存经过用户标记的结果
    if status_bak:  # a re-compute model
        try:
            logger.info('加载上次计算结果...')
            with conn.cursor() as cr:
                cr.execute(
                    f'select db1, table1, column1, db2, table2, column2 from analysis_results '
                    f'where model="{model_id}"')
                res = cr.fetchall()
                for r_ in res:
                    last_rel_res.append("".join(r_))
                cr.execute(
                    f'select db1, table1, column1, db2, table2, column2 from analysis_results '
                    f'where model="{model_id}" and `scantype` != 0')
                res = cr.fetchall()
                for r_ in res:
                    user_rel_res.append("".join(r_))
            conn.commit()
            logger.info('加载结果完成')
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            logger.info('开始回滚...')
            roll_back(status_bak, conn, model_id)
            logger.info('回滚出错')
            return {'state': 0, 'msg': '加载旧结果时出错。'}

    # 初始化算子
    progress = Progress(kwargs['tar_host'], int(kwargs['tar_port']), kwargs['tar_user'], kwargs['tar_passwd'],
                        kwargs['tar_db'], kwargs['tar_type'], model_id, logger, custom_para)

    logger.info('连接数据库')
    conn = progress.connect()
    if conn is None:
        logger.error('无法连接数据库')
        return
    logger.info('获取表名')
    if eval(kwargs['tar_tables']):
        logger.info('用户指定了表')
        tables = eval(kwargs['tar_tables'])
    else:
        logger.info('用户未指定表，获取数据库所有表')
        tables = progress.get_table_names(conn)

    logger.info('计算主键与关联字段')
    rel_cols, length_normal, _, _, _, pks, _ = progress.get_pk_cols(conn, tables)

    logger.info('生成过滤器文件')
    progress.create_bloom_filter(conn, pks, length_normal)

    logger.info('查找关系')
    output = progress.find_rel(pks, rel_cols, length_normal, conn)

    if output.empty:
        logging.info('结果为空')
        roll_back(status_bak, conn, model_id)
        logging.info(f'计算完成')
    else:
        # 删除旧版结果
        with conn.cursor() as cr:
            cr.execute(f'delete from analysis_results where model="{model_id}" and `scantype`=0')
        # num_new_rel = res_to_db(output, config_map, last_rel_res, logging)
        num_new_rel = res_to_db2(output, conn, last_rel_res, logging)
        num_rel = len(output)

        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sql = f'update analysis_status set analysis_status="2", relation_num={num_rel}, ' \
              f'new_relation_num={num_new_rel},' \
              f'end_time="{end_time}" where id="{model_id}"'
        with conn.cursor() as cr:
            cr.execute(sql)
            conn.commit()
        logging.info(f'计算完成')
    conn.close()


class Progress:
    """将整个过程封装成一个类"""

    def __init__(self, host, port, user, passwd, db, db_type, model_id, logger, custom_para):
        """实例化一个算子时需要传入的参数

        :param host: ip
        :param port: 端口
        :param user: 用户名
        :param passwd: 密码
        :param db: 数据库/实例名称
        :param db_type: 数据库类别
        :param model_id: 元数据的id
        :param logger: 日志记录器
        :param custom_para: 用户配置的参数
        """
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.db_type = db_type
        self.model_id = model_id
        self.logger = logger
        self.use_str_len, self.data_cleansing, self.inf_dup_ratio, self.inf_str_len, self.inf_tab_len = custom_para
        if self.db_type.upper() in ('MYSQL', 'GBASE'):
            self.sql1 = 'select table_name from information_schema.tables where table_schema="%s" ' \
                        'and table_type="BASE TABLE"'
            self.sql2 = 'select column_name, data_type from information_schema.columns where ' \
                        'table_schema="%s" and table_name="%s"'
            self.sql3 = 'select count(`%s`) from `%s`.`%s`'
            self.sql4 = 'select count(distinct `%s`) from `%s`.`%s`'
            self.sql5 = 'select `%s` from `%s`.`%s`'
            self.sql6 = self.sql5 + ' limit 10000'
            self.sql7 = 'select count(1) from `%s`.`%s`'
            self.sql8 = 'select count(1) from `%s`.`%s` where length(`%s`)=char_length(`%s`)'
            self.sql9 = 'select update_time from information_schema.tables where table_schema="%s" ' \
                        'and table_name="%s"'
            self.type_list = mysql_type_list
        elif self.db_type.upper() == 'ORACLE':
            self.sql1 = 'select table_name from all_tables where owner="%s"'
            self.sql2 = 'select column_name, data_type from all_tab_columns where owner="%s" ' \
                        'and table_name="%s"'
            self.sql3 = 'select count("%s") from "%s"."%s"'
            self.sql4 = 'select count(distinct "%s") from "%s"."%s"'
            self.sql5 = 'select "%s" from "%s"."%s"'
            self.sql6 = self.sql5 + ' where rownum < 10000'
            self.sql7 = 'select count(1) from "%s"."%s"'
            self.sql8 = 'select count(1) from "%s"."%s" where length("%s")=lengthb("%s")'
            self.sql9 = 'select last_ddl_time from user_objects where object_type="TABLE" and ' \
                        'object_name="%s"'
            self.type_list = oracle_type_list
        elif self.db_type.upper() == 'POSTGRESQL':
            self.sql1 = f"select tablename from pg_tables where schemaname='{self.db}'"
            self.sql2 = f'select a.attname as name, substring(format_type(a.atttypid, a.atttypmod) from ' \
                        f'"[a-zA-Z]*") as ' \
                        f'type from pg_class as c, pg_attribute as a, pg_namespace as p where ' \
                        f'c.relnamespace=p.oid ' \
                        f'and c.relname="%s" and a.attrelid=c.oid and a.attnum > 0 and p.nspname="{self.db}"'
            self.sql3 = f'select count("%s") from {self.db}.%s'
            self.sql4 = f'select count(distinct "%s") from {self.db}.%s'
            self.sql5 = f'select "%s" from {self.db}.%s'
            self.sql6 = f'select "%s" from {self.db}.%s '
            self.sql7 = f'select count(1) from {self.db}.%s'
            self.sql8 = f'select count(*) from {self.db}.%s where length("%s") = ("%s")'
        else:
            self.logger.error('不支持的数据库类型')
            return

    def connect(self):
        if self.db_type.upper() in ('MYSQL', 'GBASE'):
            conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.passwd, db=self.db)
        elif self.db_type.upper() == 'ORACLE':
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.db)
            conn = cx_Oracle.connect(self.user, self.passwd, dsn, encoding='UTF-8')
        elif self.db_type.upper() == 'POSTGRESQL':
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user,
                                    password=self.passwd, database=self.db)
        else:
            self.logger.error('不支持的数据库类型')
            return None
        return conn

    def get_table_names(self, conn):
        """获取所有的表的名称"""
        with conn.cursor() as cr:
            cr.execute(self.sql1)
            tables = cr.fetchall()
        return [i[0] for i in tables]

    def get_pk_cols(self, conn, tables):
        """遍历以确定主键和可关联字段"""
        if use_cache:
            self.logger.info("使用缓存")
            cache_file_path = self.model_id + '/' + self.db
            caches = get_cache_files(cache_file_path)
            if not caches:
                self.logger.info("未发现可用的缓存文件")
                rel_cols_ = length_normal_ = length_too_long_ = last_update_time_ = pks_ = {}
                length_zero_ = without_pks_ = no_exist_ = []
            else:
                rel_cols_, length_normal_, length_too_long_, length_zero_, without_pks_, \
                    pks_, no_exist_, last_update_time_ = caches
        else:
            self.logger.info("不使用缓存")
            rel_cols_ = length_normal_ = length_too_long_ = last_update_time_ = pks_ = {}
            length_zero_ = without_pks_ = no_exist_ = []

        rel_cols = {}  # 存储表及其可能与其他表主键进行关联的字段
        length_normal = {}  # 存储表及其长度
        length_too_long = {}  # 存储表及其长度；行数太长，可能导致内存溢出无法计算
        length_zero = []  # 空表
        without_pks = []  # 没有主键的表
        pks = {}  # 存储表及其可能的主键列表
        no_exist = []  # 数据库中不存在的表
        last_update_time = {}  # 每一张表的上次更新时间
        cr = conn.cursor()
        tab_num = len(tables)
        for i in range(tab_num):
            tab = tables[i]
            self.logger.info(f'{i + 1}/{tab_num}: `{tab}` 正在计算...')
            try:
                cr.execute(self.sql9)
                update_time = str(cr.fetchone()[0])
            except:
                update_time = None
            last_update_time[tab] = update_time
            t = last_update_time_.get(tab)
            if t:
                if t == update_time:
                    if rel_cols_.get(tab):
                        rel_cols[tab] = rel_cols_.get(tab)
                    if length_normal_.get(tab):
                        length_normal[tab] = length_normal_.get(tab)
                    if length_too_long_.get(tab):
                        length_too_long[tab] = length_too_long_.get(tab)
                    if pks_.get(tab):
                        pks[tab] = pks_.get(tab)
                    if tab in length_zero_:
                        length_zero.append(tab)
                    if tab in without_pks_:
                        without_pks.append(tab)
                    if tab in no_exist_:
                        no_exist.append(tab)
                    self.logger.info(f'`{tab}` 获取到缓存信息')
                    continue
            self.logger.info(f'`{tab}`未找到可用的缓存信息')
            self.logger.info('获取表的长度...')
            try:
                cr.execute(self.sql7 % (self.db, tab))
                row_num = cr.fetchone()[0]
                if row_num > 1e8:
                    length_too_long[tab] = row_num
                    self.logger.warning(f'{tab} 超长，并被过滤')
                    continue
                elif row_num == 0:
                    length_zero.append(tab)
                    self.logger.warning(f'{tab} 为空，并被过滤')
                    continue
                else:
                    length_normal[tab] = row_num
                    self.logger.info(f'{tab} 完成')
            except Exception as e:
                self.logger.error(f'{tab} 不存在: {e}')
                no_exist.append(tab)
                continue

            self.logger.info('获取字段名称及数据类型...')
            cr.execute(self.sql2 % (self.db, tab))
            field_and_type = cr.fetchall()
            psb_pk, psb_col = [], []
            for j in range(len(field_and_type)):
                field_name = field_and_type[j][0]
                field_type = field_and_type[j][1]
                if field_type.upper() not in self.type_list:
                    continue
                if not col_name_filter(tab, field_name, self.data_cleansing):
                    continue
                try:
                    cr.execute()
                except Exception as e:
                    logging.error(e)
                    logging.error(self.sql3 % (field_name, self.db, tab))
                    continue
                num1 = cr.fetchone()[0]
                if num1 == row_num:
                    cr.execute(self.sql4 % (field_name, self.db, tab))
                    num2 = cr.fetchone()[0]
                    cr.execute(self.sql8 % (self.db, tab, field_name, field_name))
                    num3 = cr.fetchone()[0]
                    if num1 == num2 and num2 == num3:
                        psb_pk.append(field_name)
                    if both_roles:
                        psb_col.append(field_name)
                    elif num1 == num3:
                        psb_col.append(field_name)
            if len(psb_pk):
                pks[tab] = psb_pk
            else:
                without_pks.append(tab)
            rel_cols[tab] = psb_col
        cr.close()
        for v in ['rel_cols', 'length_normal', 'length_too_long', 'length_zero',
                  'without_pks', 'pks', 'no_exist', 'last_update_time']:
            with open(f'./table_attr/{self.model_id}/{self.db}/{v}.json', 'w') as f:
                json.dump(eval(v), f)
        return rel_cols, length_normal, length_too_long, length_zero, without_pks, pks, no_exist

    def create_bloom_filter(self, conn, pks, length_normal):
        """对主键字段生成过滤器文件"""
        if not os.path.exists(f'./filters/{self.model_id}/{self.db}'):
            os.makedirs(f'./filters/{self.model_id}/{self.db}')
        total_num = sum(list(map(lambda x: len(pks[x]), list(pks))))
        self.logger.info(f"最多{total_num}个过滤器需要被创建")
        try:
            with open(f'./filters/{self.model_id}/{self.db}/last_create_time.json') as f:
                last_create_time = json.load(f)
        except FileNotFoundError:
            last_create_time = {}
        with open(f'./table_attr/{self.model_id}/{self.db}/last_update_time.json') as f:
            last_update_time = json.load(f)

        n = 1
        for i in range(len(pks)):
            tab = list(pks.keys())[i]
            capacity = int(length_normal[tab] * 1.2)
            pk_list = pks[tab]
            for k in range(len(pk_list)):
                pk = pk_list[k]
                self.logger.info(f'{n:4}/{total_num:4}:Computing {tab}.{pk}, {length_normal[tab]} rows in total.')
                filter_name = tab + '@' + pk + '.filter'
                if os.path.exists(f'./filters/{self.model_id}/{self.db}/{filter_name}'):
                    if last_create_time.get(tab) == last_update_time[tab]:
                        self.logger.info(f'{" " * 9}{tab}.{pk} already exists, continue')
                        n += 1
                        continue
                    else:
                        last_create_time[tab] = last_update_time[tab]
                value = pd.read_sql(f"""select {pk} from {tab}
                        """, conn)
                if value.shape[0] > capacity:
                    capacity = int(value.shape[0] * 1.2)
                if multi_process:
                    bf = add_operation(value, capacity)
                else:
                    bf = BloomFilter(capacity)
                    for j in value.iloc[:, 0]:
                        bf.add(j)
                with open(f'./filters/{self.model_id}/{self.db}/{filter_name}', 'wb') as f:
                    pickle.dump(bf, f)
                n += 1

    def find_rel(self, pks, rel_cols, length_normal, conn):
        """查找关系"""
        n = 1
        results = []
        tables = list(set(pks.keys()).union(set(rel_cols.keys())))
        for i in tables:
            self.logger.info(f'{n:4}/{len(tables)}:计算 `{i}`...')
            n += 1
            if i in not_cite_table_list:
                continue
            if length_normal[i] < self.inf_tab_len:
                continue
            if not rel_cols.get(i):
                continue
            for col in rel_cols[i]:
                try:
                    value = pd.read_sql(self.sql6 % (col, self.db, i), conn)
                    all_num = value.shape[0]
                except Exception as e:
                    self.logger.error(e)
                    continue
                df = col_value_filter(value, int(self.use_str_len), int(self.inf_str_len), float(self.inf_dup_ratio))
                if isinstance(df, pd.DataFrame) and df.empty:
                    continue
                if df is None:
                    continue
                for pk_tab in pks:
                    if pk_tab in not_base_table_list:
                        continue
                    if pk_tab == i:
                        continue
                    for pk in pks[pk_tab]:
                        with open(f'./filters/{self.model_id}/{self.db}/{pk_tab}@{pk}.filter', 'rb') as f:
                            bf = pickle.load(f)
                        flag = 1
                        num_not_in_bf = 0
                        for k in value[col]:
                            if k not in bf:
                                num_not_in_bf += 1
                            if num_not_in_bf / all_num > sup_out_foreign_key:
                                flag = 0
                                break
                        if flag:
                            not_match_ratio = num_not_in_bf / all_num
                            res = [self.model_id, self.db, pk_tab, 'table-comment',
                                   pk, 'column-comment', self.model_id, self.db, i,
                                   'table-comment', col, 'column-comment', not_match_ratio]
                            results.append(res)
        return results


class UnSupportedDbError(Exception):
    pass

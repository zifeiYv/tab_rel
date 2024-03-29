# -*- coding: utf-8 -*-
import logging
import time
import traceback
import requests

import pymysql

from .utils import save_to_db, col_value_filter, col_name_filter


def main(**kwargs):
    """计算关系的主函数。

    可选的参数包括：
    model_id, notify_url, execute_obj, alg_name,
    cfg_db, cfg_host, cfg_passwd, cfg_port, cfg_user,
    tar_type, tar_db, tar_host,
    tar_passwd, tar_port, tar_user, tar_url
    """
    model_id = kwargs['model_id']
    execute_obj = kwargs['execute_obj']
    alg_name = kwargs['alg_name']

    logger = logging.getLogger(model_id)

    logger.info(f"""参数信息：
    model_id: {model_id},
    cfg_db: {kwargs['cfg_db']},
    cfg_host: {kwargs['cfg_host']},
    cfg_passwd: {kwargs['cfg_passwd']},
    cfg_port: {kwargs['cfg_port']},
    cfg_user: {kwargs['cfg_user']},

    tar_type: {kwargs['tar_type']},
    tar_db: {kwargs['tar_db']},
    tar_host: {kwargs['tar_host']},
    tar_passwd: {kwargs['tar_passwd']},
    tar_port: {kwargs['tar_port']},
    tar_user: {kwargs['tar_user']},
    tar_url: {kwargs['tar_url']},

    notify_url: {kwargs['notify_url']}
    """)
    logger.info('初始化状态表...')
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Step 1 初始化模型的状态表。
    # 注意:
    #   - 对于一个新建的模型，结果表中是不存在旧结果的，因此，程序会在状态表中插入一条新记录，字段
    #     "analysis_status"的值为1（代表"计算中"），并且在计算完成后将这个值改写为2（代表"计算完成"）。
    #     如果在计算过程中出现来一些错误，那么程序会删除这一条新增的记录。
    #   - 对于一个已经计算过关系的模型，结果表中可能存在旧结果，因此，重新计算时，程序会将旧结果全部删除（用户
    #     指定的某些结果会得到保留）。另外，由于已经计算过，所以状态表中字段"analysis_status"的值为2，程序首先
    #     将其改写为1，如果计算完成，再改写为1。如果过程中出错，那么将把该值重新恢复成2。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    cfg = {
        'user': kwargs['cfg_user'],
        'host': kwargs['cfg_host'],
        'port': int(kwargs['cfg_port']),
        'passwd': kwargs['cfg_passwd'],
        'db': kwargs['cfg_db']
    }
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        conn = pymysql.connect(**cfg)
    except pymysql.err.OperationalError:
        logger.error(traceback.format_exc())
        logger.error(f'无法连接配置数据库{cfg}')
        return
    sql = f'REPLACE INTO analysis_status (id, analysis_status, algorithm_name, execute_obj,' \
          f'start_time) values("{model_id}", "1", "{alg_name}", "{execute_obj}", "{start_time}")'
    with conn.cursor() as cr:
        cr.execute(f"select t.analysis_status from analysis_status t where t.id='{model_id}'")
        res = cr.fetchone()
        cr.execute(sql)
        conn.commit()
    if not res:
        status_bak = False
        logger.info('模型首次参与运行')
    else:
        status_bak = res[0]
        logger.info('模型已经存在运算结果')

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Step 2 获取用户参数
    #   "custom parameters"记录了一些临界值，这些临界值对于计算结果的准确性是有非常大的影响的。用户在充分理解
    #   各个值的作用的情况下，调整这些值有利于使结果更加合理。
    #   如果用户不加修改，那么会采用默认值。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    logger.info('获取用户参数...')
    sql = f'select t.csl, t.ds, t.idr, t.isl, t.itl, t.tables1, t.tables2 from ' \
          f'pf_user_config t where t.model="{model_id}{model_id}"'
    with conn.cursor() as cr:
        try:
            if cr.execute(sql):
                logger.info('获取用户参数成功。')
                res = cr.fetchone()
                use_str_len = str(res[0]) if res[0] is not None else '0'
                data_cleansing = eval(str(res[1])) if res[1] else {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = res[2] if res[2] is not None else 0.4
                inf_str_len = res[3] if res[3] is not None else 3
                inf_tab_len = res[4] if res[4] is not None else 10
                tar_tables = list(res[5].split(',')) if res[5] else []
            else:
                logger.info('当前未指定参数，将采用默认值。')
                use_str_len = '0'
                data_cleansing = {'_': ['EXT_', 'ext_']}
                inf_dup_ratio = 0.3
                inf_str_len = 1
                inf_tab_len = 5
                tar_tables = []
        except:
            logger.warning('获取参数的SQL执行错误。')
            use_str_len = '0'
            data_cleansing = {'_': ['EXT_', 'ext_']}
            inf_dup_ratio = 0.3
            inf_str_len = 1
            inf_tab_len = 5
            tar_tables = []
        # Merge custom parameters to a tuple
        custom_para = (use_str_len, data_cleansing, inf_dup_ratio, inf_str_len, inf_tab_len)
    logger.info('完成参数获取')
    logger.info(f"""
    使用的参数为：
    use_str_len:    {use_str_len},
    data_cleansing: {data_cleansing},
    inf_dup_ratio:  {inf_dup_ratio},
    inf_str_len:    {inf_str_len},
    inf_tab_len:    {inf_tab_len}
    """)

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Step 3 开始计算
    #   上述步骤完成后，开始读取数据进行计算。
    #   如果模型并非首次进行运算，那么在写入新的结果前会把部分旧的结果删除。
    #   如果旧的结果是用户手动添加的或者经过了用户的编辑或是用户删除的数据，那么在结果表中的"scantype"字段
    #   会以"1"（数值型）标示；否则，以"0"（数值型）标示。在执行删除操作时，只会删除"scantype"取值为"0"的记录。
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    last_rel_res = []  # 缓存上次计算的结果
    # user_rel_res = []  # 缓存经过用户标记的结果
    if status_bak:  # a re-compute model
        logger.info('加载上次计算结果...')
        with conn.cursor() as cr:
            cr.execute(
                f'select db1, table1, column1, db2, table2, column2 from analysis_results '
                f'where model="{model_id}"')
            res = cr.fetchall()
            for r_ in res:
                last_rel_res.append("".join(map(str, r_)))
        #     cr.execute(
        #         f'select db1, table1, column1, db2, table2, column2 from analysis_results '
        #         f'where model="{model_id}" and `scantype` != 0')
        #     res = cr.fetchall()
        #     for r_ in res:
        #         user_rel_res.append("".join(r_))
        # conn.commit()
        logger.info('结果加载完成')

    # tar_tables = eval(kwargs['tar_tables']) if eval(kwargs['tar_tables']) else None

    if kwargs['tar_type'].upper() == 'MYSQL':
        from .mysql import run
        output = run(model_id, tar_tables, custom_para,
                     host=kwargs['tar_host'],
                     port=int(kwargs['tar_port']),
                     user=kwargs['tar_user'],
                     passwd=kwargs['tar_passwd'],
                     db=kwargs['tar_db'])
    elif kwargs['tar_type'].upper() == 'GBASE':
        from .gbase import run
        output = run(model_id, tar_tables, custom_para,
                     host=kwargs['tar_host'],
                     port=int(kwargs['tar_port']),
                     user=kwargs['tar_user'],
                     passwd=kwargs['tar_passwd'],
                     db=kwargs['tar_db'])
    elif kwargs['tar_type'].upper() == 'ORACLE':
        from .oracle import run
        output = run(model_id, tar_tables, custom_para,
                     host=kwargs['tar_host'],
                     port=int(kwargs['tar_port']),
                     user=kwargs['tar_user'],
                     passwd=kwargs['tar_passwd'],
                     url=kwargs['tar_url'],
                     db=kwargs['tar_db']
                     )
    elif kwargs['tar_type'].upper() == 'HIVE':
        from .hive import run
        output = run(model_id, tar_tables, custom_para,
                     host=kwargs['tar_host'],
                     port=int(kwargs['tar_port']),
                     user=kwargs['tar_user'],
                     passwd=kwargs['tar_passwd'],
                     url=kwargs['tar_url'],
                     db=kwargs['tar_db']
                     )
    elif kwargs['tar_type'].upper() == 'POSTGRESQL':
        from .pg import run
        output = run(model_id, tar_tables, custom_para,
                     host=kwargs['tar_host'],
                     port=int(kwargs['tar_port']),
                     user=kwargs['tar_user'],
                     passwd=kwargs['tar_passwd'],
                     db=kwargs['tar_db'])
    elif kwargs['tar_type'].upper() == 'SQLSERVER':
        from .mssql import run
        output = run(model_id, tar_tables, custom_para,
                     host=kwargs['tar_host'],
                     port=int(kwargs['tar_port']),
                     user=kwargs['tar_user'],
                     passwd=kwargs['tar_passwd'],
                     db=kwargs['tar_db'])
    else:
        output = None

    if output is None:
        logger.warning('不支持的数据库类型')
    else:
        if output.empty:
            logger.info('结果为空')
        else:
            # 删除旧版结果
            with conn.cursor() as cr:
                cr.execute(f'delete from analysis_results where model="{model_id}" and `scantype`=0')
            num_new_rel = save_to_db(output, conn, last_rel_res)
            num_rel = len(output)

            end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            sql = f'update analysis_status set analysis_status="2", relation_num={num_rel}, ' \
                  f'new_relation_num={num_new_rel},' \
                  f'end_time="{end_time}" where id="{model_id}"'
            with conn.cursor() as cr:
                cr.execute(sql)
                conn.commit()
    logger.info(f'计算完成')
    conn.close()
    logger.info('回调……')
    try:
        requests.get(kwargs['notify_url'] + '?modelId=' + model_id)
        logger.info('成功')
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        logger.error('失败')

# -*- coding: utf-8 -*-
import argparse

from utils import main
from utils.utils import init_logger


if __name__ == '__main__':
    # 将所有的参数都已str的类型传入，然后再进行处理
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_id', help="元数据的唯一标识")
    parser.add_argument('--notify_url')
    parser.add_argument('--execute_obj', help="转存一下")
    parser.add_argument('--alg_name', help='转存一下')
    parser.add_argument('--cfg_db', help='存储结果的数据库的名称')
    parser.add_argument('--cfg_host', help='存储结果的数据库的ip')
    parser.add_argument('--cfg_passwd', help='存储结果的数据库的密码')
    parser.add_argument('--cfg_port', help='存储结果的数据库的端口号')
    parser.add_argument('--cfg_user', help='存储结果的数据库的用户名')

    parser.add_argument('--tar_type', help='目标数据源的数据库类型')
    parser.add_argument('--tar_db', help='目标数据源的数据库的名称')
    parser.add_argument('--tar_host', help='目标数据源的ip')
    parser.add_argument('--tar_passwd', help='目标数据源的密码')
    parser.add_argument('--tar_port', help='目标数据源的端口号')
    parser.add_argument('--tar_user', help='目标数据源的用户名')
    parser.add_argument('--tar_url', help='仅用于oracle数据库')

    args = parser.parse_args()
    model_id = args.model_id
    notify_url = args.notify_url
    execute_obj = args.execute_obj
    alg_name = args.alg_name
    cfg_db = args.cfg_db
    cfg_host = args.cfg_host
    cfg_passwd = args.cfg_passwd
    cfg_port = args.cfg_port
    cfg_user = args.cfg_user
    tar_type = args.tar_type
    tar_db = args.tar_db
    tar_host = args.tar_host
    tar_passwd = args.tar_passwd
    tar_port = args.tar_port
    tar_user = args.tar_user
    tar_url = args.tar_url
    init_logger(model_id)
    main(model_id=model_id, notify_url=notify_url, execute_obj=execute_obj, alg_name=alg_name,
         cfg_db=cfg_db, cfg_host=cfg_host, cfg_passwd=cfg_passwd, cfg_port=cfg_port, cfg_user=cfg_user,
         tar_type=tar_type, tar_db=tar_db, tar_host=tar_host, tar_passwd=tar_passwd,
         tar_port=tar_port, tar_user=tar_user, tar_url=tar_url)

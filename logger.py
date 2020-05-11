# encoding:utf-8
import logging
from logging.handlers import RotatingFileHandler
import os
import platform


def get_logger(model_id):
    """
    log_file：一个日志文件的名称，如包含路径则必须事先创建
    """
    cwd = os.getcwd()
    path = os.path.join(cwd, 'logs', model_id)
    if not os.path.isdir(path):
        os.makedirs(path)
    if platform.system() == 'Windows':
        log_filename = path + '\\relation.out'
    else:
        log_filename = path + '/relation.out'

    logger = logging.getLogger(model_id)
    logger.setLevel(logging.DEBUG)  # 最低显示级别为DEBUG
    handler = RotatingFileHandler(log_filename, maxBytes=10*1024*1024, backupCount=5)
    # 输出格式设置
    formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger, handler

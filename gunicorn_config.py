# -*- coding: utf-8 -*-
import os
import shutil

if os.path.exists('./logs'):
    shutil.rmtree('./logs')
os.mkdir('./logs')
# 启动方式
# gunicorn -c gunicorn_config.py app:app
#
# 监听的ip与端口
bind = '0.0.0.0:5002'

# 进程数量
# Gunicorn should only need 4-12 worker processes to handle hundreds or thousands of requests per second.
workers = 2 * os.cpu_count() + 1

# 日志处理
accesslog = './logs/info_log'

errorlog = './logs/error_log'


loglevel = 'warning'

# 设置timeout时间
timeout = 600

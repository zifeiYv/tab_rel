# -*- coding: utf-8 -*-
import os
import shutil

# 创建存储日志的文件夹
if os.path.exists('./logs'):
    shutil.rmtree('./logs')
os.mkdir('./logs')

# 启动方式
# gunicorn -c gunicorn_config.py app:app

# 监听的ip与端口
bind = '0.0.0.0:5002'

# 进程数量
# Gunicorn should only need 4-12 worker processes to handle hundreds or thousands of requests per second.
workers = 2 * os.cpu_count() + 1
# 线程数量
threads = 2

# 日志处理
# 请求记录日志
accesslog = './logs/info_log'
# 错误记录日志
errorlog = './logs/error_log'
# 错误日志的级别
loglevel = 'warning'

# 将标准输出重定向到错误文件中
capture_output = True

# 设置进程的timeout时间
timeout = 600

daemon = True

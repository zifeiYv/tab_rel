# -*- coding: utf-8 -*-
"""
#-------------------------------------------------------------------#
#                    Project Name : code                            #
#                                                                   #
#                       File Name : gunicorn_config.py              #
#                                                                   #
#                          Author : Jiawei Sun                      #
#                                                                   #
#                          Email : j.w.sun1992@gmail.com            #
#                                                                   #
#                      Start Date : 2020/07/21                      #
#                                                                   #
#                     Last Update :                                 #
#                                                                   #
#-------------------------------------------------------------------#
"""
# 启动方式
# gunicorn -c gunicorn_config.py app:app
#
# 监听的ip与端口
bind = '127.0.0.1:5002'

# 进程数量
# workers = 4

# 日志处理
accesslog = './logs/info_log'

errorlog = './logs/error_log'


loglevel = 'warning'


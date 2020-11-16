# -*- coding: utf-8 -*-
"""
@Time       : 2020/2/7 16:42
@Author     : Jarvis
@Annotation : Sorry for this shit code
"""
from config import url, redis_config
from flask import Flask, request, abort, jsonify
from utils import del_cache_files
from main_utils import main_process, R
from concurrent.futures import ProcessPoolExecutor
import redis

app = Flask(__name__)
executor = ProcessPoolExecutor(1)
try:
    r = redis.Redis(**redis_config)
    r.ping()
except Exception as e:
    print(e)
    print('Can not connect to redis and progress is not available.')
    r = R()


@app.route(url + 'calculation/', methods=["POST"])
def find_relation():
    if not request.json:
        return jsonify({'state': 0, 'msg': 'Invalid parameter format, json-format needed!'})
    post_json = request.json
    """
    The format of valid parameter posted by user is as follows:
        {
            "algorithmName": "表关系分析算法",
            "configMap": {  # 配置数据库的信息
                "db": "dmm_test",
                "host": "191.168.6.103",
                "password": "merit",
                "port": 3306,
                "user": "root"
            },
            "dbInfo": {  # 数据源的信息
                "config": {
                    "db": "dmm_test",
                    "host": "191.168.6.103",
                    "password": "merit",
                    "port": 3306,
                    "user": "root",
                    "url": "localhost:1521/orcl",  # 仅用于oracle数据库
                    "multi_schema": False,  # 是否为多模式，仅用于oracle数据库
                    "target_schema": ""  # 引用的模式，仅用于oracle数据库
                    },
                "type": "mysql",
                "tables: []  # 存储指定表进行融合
            },
            "notifyUrl":"http://1121",
            "executObj": "local_db v1版本",
            "modelId": "92ae9b17770041ae85e563cf95c9cf56"
        }
            
    `algorithmName` and `executObj` are NOT used by this program and just stored into specific position.
    `configMap` includes the key information to connect config database.
    """
    # executor.submit(main_process, post_json)
    main_process(post_json)
    return jsonify({'state': 1, 'msg': "Valid parameters and computation started."})


@app.route(url + 'get_progress/')
def get_progress():
    if isinstance(r, R):
        return jsonify({'stage': 0, 'progress': '无法启动进度功能'})
    return jsonify({'stage': str(r.get('stage'), 'utf-8'),
                    'progress': round(float(str(r.get('progress'), 'utf-8')), 2),
                    'state': int(str(r.get('state'), 'utf-8')), 'msg': str(r.get('msg'), 'utf-8')})


@app.route("/update_parameter_config/", methods=['POST'])
def update_parameter():
    if not request.json:
        abort(404)
    post_json = request.json
    # Delete cached files before updating parameters
    # New parameters will be stored into MySQL
    result = del_cache_files(post_json)
    if not result:
        return jsonify({'state': 0, 'msg': "清理缓存失败！"})
    return jsonify({'msg': 'Analyze completed'})


@app.route(url + 'finish/')
def func():
    model_id = request.args.get('model_id')
    return f"Finish {model_id}"


if __name__ == '__main__':
    # 正式环境下用以下命令启动服务
    # gunicorn -c gunicorn_config.py app:app
    app.run('0.0.0.0', port=5002, debug=True)

# -*- coding: utf-8 -*-
"""
@Project: table_relation_new
@Time   : 2019/8/7 15:13
@Author : sunjw

以post方式请求服务，以application/json的形式传参，参数格式如下：
{
    "modelId1": "模型1的id",
    "modelId2": "模型2的id",
    "configMap": {          # 配置数据库的连接信息，固定为mysql库
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "passwd",
        "db": "db_model"
    }
}
"""
from flask import Flask, request, abort, make_response, jsonify
from utils import run_process, del_cache_file
from ParameterChecker import parameter_checker

app = Flask(__name__)
url = '/all_tables_relation/'


@app.route(url, methods=['POST'])
def task():
    if not request.json:
        abort(404)
    post_json = request.json
    # 参数校验
    if parameter_checker(post_json):
        return jsonify({'state': 0, 'msg': parameter_checker(post_json)})

    run_process(post_json)
    return jsonify({'msg': 'Analyze completed'})


@app.route("/update_parameter_config", methods=['POST'])
def task():
    if not request.json:
        abort(404)
    post_json = request.json
    # 参数校验
    result = del_cache_file(post_json)
    if result == "0":
        return jsonify({'state': 0, 'msg': "清理缓存失败！"})
    return jsonify({'msg': 'Analyze completed'})


@app.errorhandler(404)
def wrong_para():
    return make_response(jsonify({'error': 'wrong parameter type!'}), 404)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)

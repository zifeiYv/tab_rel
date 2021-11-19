# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
from subprocess import Popen

app = Flask(__name__)
url = '/all_tables_relation/'


@app.route('/all_tables_relation/calculation/', methods=["POST"])
def find_relation():
    """
    post参数的格式如下：
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
                "db": "dmm_test",  # oracle下为模式名
                "host": "191.168.6.103",
                "password": "merit",
                "port": 3306,
                "user": "root",
                "url": "localhost:1521/orcl",  # 仅用于oracle数据库
                },
            "type": "mysql"
        },
        "notifyUrl":"http://1121",
        "executObj": "local_db v1版本",
        "modelId": "92ae9b17770041ae85e563cf95c9cf56",
        "modelId2": "92ae9b17770041ae85e563cf95c9cf56",  # not used
        "modelAnilyType": "2",  # not used
    }

    `algorithmName`与`executObj`并不会被本算法使用，只是做了一次转存
    """
    if not request.json:
        return jsonify({'state': 0, 'msg': 'Invalid parameter format, json-format needed!'})
    args = request.json
    # 第一层的参数
    model_id = args['modelId']
    notify_url = args['notifyUrl']
    execute_obj = args['executObj']
    alg_name = args['algorithmName']

    # 第二层的参数
    config_map = args['configMap']
    db_info = args['dbInfo']
    # # 存储结果的配置库的信息
    cfg_db = config_map['db']
    cfg_host = config_map['host']
    cfg_passwd = config_map['password']
    cfg_port = str(config_map['port'])
    cfg_user = config_map['user']
    # # 目标数据源的信息
    tar_type = db_info['type']
    tar_db = db_info['config']['db'] if db_info['config']['db'] else ''
    tar_host = db_info['config']['host']
    tar_passwd = db_info['config']['password']
    tar_port = str(db_info['config']['port'])
    tar_user = db_info['config']['user']
    # >>> 以下仅用于Oracle数据库
    try:
        tar_url = db_info['config']['url']
    except KeyError:
        tar_url = ''
    # <<< 以上仅用于Oracle数据库

    Popen(['python', 'run.py',
           '--model_id', model_id,
           '--notify_url', notify_url,
           '--execute_obj', execute_obj,
           '--alg_name', alg_name,
           '--cfg_db', cfg_db,
           '--cfg_host', cfg_host,
           '--cfg_passwd', cfg_passwd,
           '--cfg_port', cfg_port,
           '--cfg_user', cfg_user,
           '--tar_type', tar_type,
           '--tar_db', tar_db,
           '--tar_host', tar_host,
           '--tar_passwd', tar_passwd,
           '--tar_port', tar_port,
           '--tar_user', tar_user,
           '--tar_url', tar_url
           ])

    return jsonify({'state': 1, 'msg': "Valid parameters and computation started."})


@app.route('/update_parameter_config/', methods=['POST'])
def update():
    return jsonify({'state': 1, 'msg': 'nothing to do'})


if __name__ == '__main__':
    # 正式环境下用以下命令启动服务
    # gunicorn -c gunicorn_config.py app:app
    app.run('0.0.0.0', port=5002, debug=True)

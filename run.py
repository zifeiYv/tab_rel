# -*- coding: utf-8 -*-
"""
@Time       : 2020/2/7 16:42
@Author     : Jarvis
@Annotation : Sorry for this shit code
"""
from config import url, port
from flask import Flask, request, abort, make_response, jsonify
from utils import del_cache_files
from main_utils import main_process


app = Flask(__name__)


@app.route(url, methods=["POST"])
def find_relation():
    if not request.json:
        return jsonify({'state': '0', 'msg': 'Invalid parameter format, json-format needed!'})
    post_json = request.json
    """
    The format of valid parameter posted by user is as follows:
        {
            "modelId1": "<model-id>",
            "modelId2": "<another-model-id>",
            "algorithmName": "<just-receive-it-and-write-into-database>",
            "executObj": "<just-receive-it-and-write-into-database>",
            "configMap": {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "<some-passwd>"
                "db": "db"
            }
        }
    
    `algorithmName` and `executObj` are NOT used by this program and just stored into specific position.
    `configMap` includes the key information to connect config database.
    """
    res = main_process(post_json)
    return jsonify(res)


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


if __name__ == '__main__':
    app.run('0.0.0.0', port=port)

import os
import json

data = {
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
        "tables": []  # 存储指定表进行融合
    },
    "notifyUrl": "http://1121",
    "executObj": "local_db v1版本",
    "modelId": "92ae9b17770041ae85e563cf95c9cf56"
}

json_data = json.dumps(data)

if __name__ == '__main__':
    url = 'http://127.0.0.1:5002/all_tables_relation/calculation/'
    os.system('curl -H "Content-Type:application/json" -d ' + "'" + json_data + "'" + ' ' + url)

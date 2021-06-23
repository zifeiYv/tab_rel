import os
import json

data = {
    "algorithmName": "表关系分析算法",
    "configMap": {  # 配置数据库的信息
        "db": "entity_fuse",
        "host": "127.0.0.1",
        "password": "123456",
        "port": 3306,
        "user": "root"
    },
    "dbInfo": {  # 数据源的信息
        "config": {
            "db": "movie_data",
            "host": "191.168.6.242",
            "password": "merit1998",
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
    "modelId": "test1"
}

json_data = json.dumps(data)

if __name__ == '__main__':
    url = 'http://127.0.0.1:5002/all_tables_relation/calculation/'
    os.system('curl -H "Content-Type:application/json" -d ' + "'" + json_data + "'" + ' ' + url)

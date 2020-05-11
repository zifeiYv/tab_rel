# -*- coding: utf-8 -*-
"""
@Project: table_relation_new
@Time   : 2019/8/7 15:36
@Author : sunjw
"""


def parameter_checker(para):
    """
    对传入的参数的格式进行校验
    :param para: 参数，python字典格式
    :return: True or 错误信息
    """
    if 'modelId1' not in para:
        return "参数'modelId1'为必需项！"
    if not isinstance(para['modelId1'], str):
        return "'modelId1'的值必须为字符串！"
    if 'modelId2' not in para:
        return "参数'modelId2'为必需项！"
    if not isinstance(para['modelId2'], str):
        return "'modelId2'的值必须为字符串！"
    if 'configMap' not in para:
        return "参数'configMap'为必需项！"
    if not isinstance(para['configMap'], dict):
        return "'configMap'的值必须为json格式！"
    for i in ['host', 'port', 'user', 'password', 'db']:
        if i not in para['configMap']:
            return f"'{i}'必须存在configMap中！"
        if i != 'port':
            if not isinstance(para['configMap'][i], str):
                return f"configMap中的'{i}'的值必须为字符串！"
        if not isinstance(para['configMap']['port'], int):
            return "configMap中的'port'的值必须为整型！"

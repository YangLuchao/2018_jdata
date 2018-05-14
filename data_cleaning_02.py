#!/usr/bin/python
# -*- coding:utf-8 -*-


'''
只用用户商品订单表清洗数据，建模
数据清洗第二版
'''

import time
from datetime import datetime
from datetime import timedelta
import pandas as pd
import pickle
import os
import math
import numpy as np

# 定义文件名
JDATA_USER_ACTION = 'data_ori/jdata_user_action.csv'
JDATA_SKU_BASIC_INFO = 'data_ori/jdata_sku_basic_info.csv'
JDATA_USER_BASIC_INFO = 'data_ori/jdata_user_basic_info.csv'
JDATA_USER_COMMENT_SCORE = 'data_ori/jdata_user_comment_score.csv'
JDATA_USER_ORDER = 'data_ori/jdata_user_order.csv'
ITEM_ORDER_COMMENT_FILE = "clean_data/item_order_comment.csv"
ITEM_ACTION_FILE = "clean_data/item_action.csv"


def load_data(dump_path):
    '''
    加载数据
    :param dump_path:
    :return:
    '''
    return pickle.load(open(dump_path, 'rb'))


def dump_data(obj, dump_path):
    '''
    存储数据
    :param obj:
    :param dump_path:
    :return:
    '''
    pickle.dump(obj, open(dump_path, 'wb+'))


def is_exist(dump_path):
    '''
    资源是否存在
    :param dump_path:
    :return:
    '''
    return os.path.exists(dump_path)


def get_from_fname(fname):
    '''
    通过名字获取数据
    :param fname:
    :return:
    '''
    df_item = pd.read_csv(fname, header=0)
    return df_item


def get_all_sku():
    '''
    获取所有商品信息
    :return:
    '''
    dump_path = './cache/get_all_sku.pkl'
    if is_exist(dump_path):
        sku = load_data(dump_path)
    else:
        sku = get_from_fname(JDATA_SKU_BASIC_INFO)
        para_2_one_hot = pd.get_dummies(sku['para_2'], prefix='para_2')
        para_3_one_hot = pd.get_dummies(sku['para_3'], prefix='para_3')
        sku = pd.concat([sku, para_2_one_hot, para_3_one_hot], axis=1)
        del sku['para_2']
        del sku['para_3']
        del sku['cate']
        dump_data(sku, dump_path)
    return sku


def get_all_user():
    '''
    获取所有用户信息
    :return:
    '''
    dump_path = './cache/get_all_user.pkl'
    if is_exist(dump_path):
        user = load_data(dump_path)
    else:
        user = get_from_fname(JDATA_USER_BASIC_INFO)
        age_one_hot = pd.get_dummies(user['age'], prefix='age')
        sex_one_hot = pd.get_dummies(user['sex'], prefix='sex')
        user_lv_one_hot = pd.get_dummies(user['user_lv_cd'], prefix='user_lv_cd')
        user = pd.concat([user['user_id'], age_one_hot, sex_one_hot, user_lv_one_hot], axis=1)
        dump_data(user, dump_path)
    return user


def get_all_order():
    '''
    获取所有订单信息
    :return:
    '''
    dump_path = './cache/get_all_order.pkl'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_from_fname(JDATA_USER_ORDER)
        dump_data(order, dump_path)
    return order


def get_order_user_sku():
    dump_path = './cache/order_user_sku.csv'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        user = get_all_user()
        sku = get_all_sku()
        order = pd.merge(order, user, on='user_id', how='left')
        order = pd.merge(order, sku, on='sku_id', how='left')
        dump_data(order, 'cache/order_user_sku.pkl')
    return order


def get_train_test_set():

    order = get_order_user_sku()
    order = order[order['o_date'] >= '2017-04-01']
    user_sku = order[['user_id', 'sku_id']]
    del order['user_id']
    del order['sku_id']
    lables = order['o_date']
    return user_sku, order, lables


if __name__ == '__main__':
    get_train_test_set()

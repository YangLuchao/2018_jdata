#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2018/5/17
# code is far away from bugs with the god animal protecting
    I love animals. They taste delicious.
              ┏┓      ┏┓
            ┏┛┻━━━┛┻┓
            ┃      ☃      ┃
            ┃  ┳┛  ┗┳  ┃
            ┃      ┻      ┃
            ┗━┓      ┏━┛
                ┃      ┗━━━┓
                ┃  神兽保佑    ┣┓
                ┃　永无BUG！   ┏┛
                ┗┓┓┏━┳┓┏┛
                  ┃┫┫  ┃┫┫
                  ┗┻┛  ┗┻┛
"""

import os
import pickle

import pandas as pd

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


def get_all_sku_05():
    '''
    获取所有商品信息
    :return:
    '''
    dump_path = './cache/get_all_sku_05.pkl'
    if is_exist(dump_path):
        sku = load_data(dump_path)
    else:
        sku = get_from_fname(JDATA_SKU_BASIC_INFO)
        para_2_one_hot = pd.get_dummies(sku['para_2'], prefix='para_2')
        para_3_one_hot = pd.get_dummies(sku['para_3'], prefix='para_3')
        sku = pd.concat([sku, para_2_one_hot, para_3_one_hot], axis=1)
        del sku['para_2']
        del sku['para_3']
        dump_data(sku, dump_path)
    return sku


def get_all_user_05():
    '''
    获取所有用户信息
    :return:
    '''
    dump_path = './cache/get_all_user_05.pkl'
    if is_exist(dump_path):
        user = load_data(dump_path)
    else:
        user = get_from_fname(JDATA_USER_BASIC_INFO)
        dump_data(user, dump_path)
    return user


def get_all_order_05():
    '''
    获取所有订单信息
    :return:
    '''
    dump_path = './cache/get_all_order_05.pkl'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_from_fname(JDATA_USER_ORDER)
        user = get_all_user_05()
        sku = get_all_sku_05()
        order = pd.merge(order, user, how='left', on='user_id')
        order = pd.merge(order, sku, how='left', on='sku_id')
        order = order[(order['age'] != 1) & (order['age'] != 6)]
        order = order[(order['cate'] == 101) & (order['cate'] != 71) & (order['cate'] != 30)]
        # order = order.sort_values(by=['cate', 'o_date'], ascending=[True, True])
        # order = order.groupby(['user_id', 'o_date', 'cate'], as_index=False).sum()
        dump_data(order, dump_path)
    return order


if __name__ == '__main__':
    get_all_order_05()

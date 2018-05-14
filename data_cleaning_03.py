#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2018/5/14
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


def get_order_by_date(start_date, end_date):
    '''
    通过开始时间与结束时间截取订单片段
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/get_order_by_date_%s_%s.pkl' % (start_date, end_date)
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        order = order[(order.o_date >= start_date) & (order.o_date < end_date)]
        dump_data(order, dump_path)
    return order


def get_order_by_handle(start_date, end_date):
    dump_path = './cache/get_order_by_handle_02.pkl'
    if is_exist(dump_path):
        load_data(dump_path)
    else:
        order = get_order_by_date(start_date, end_date)
        del order['o_id']
        del order['o_date']
        order = order.groupby(['user_id', 'sku_id', 'o_area'], as_index=False).sum()
        order.rename(columns={'o_area': '%s_%s_o_area' % (start_date, end_date),
                              'o_sku_num': '%s_%s_o_sku_num' % (start_date, end_date)}, inplace=True)

    return order


def get_last_order():
    dump_path = './cache/get_last_order.pkl'
    if is_exist(dump_path):
        load_data(dump_path)
    else:
        order = get_all_order()


def get_train_test_set_02():
    dump_path = './cache/get_train_test_set.pkl'
    o_time = '2017-04-30'
    date_interval = [7, 14, 21, 30, 60, 90]
    actions = None
    if is_exist(dump_path):
        load_data(dump_path)
    else:
        for i in reversed(date_interval):
            # 确定结束时间，处理结束时间
            start_days = datetime.strptime(o_time, '%Y-%m-%d') - timedelta(days=i)
            start_days = start_days.strftime('%Y-%m-%d')
            if actions is None:
                actions = get_order_by_handle(start_days, o_time)
            else:
                actions = pd.merge(actions, get_order_by_handle(start_days, o_time), how='left',
                                   on=['user_id', 'sku_id'])
                actions = actions.fillna(0)

    dump_data(actions, dump_path)
    user_sku = actions[['user_id', 'sku_id']]
    del actions['user_id']
    del actions['sku_id']

    return actions, user_sku


if __name__ == '__main__':
    get_train_test_set_02()
    print('1')

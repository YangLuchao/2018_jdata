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
                  数据清洗第四版，给LSTM提供数据
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime
from datetime import timedelta
import pandas as pd
import pickle
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.dates import AutoDateLocator, DateFormatter
import math
from pandas.core.frame import DataFrame, Series
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
        dump_data(user, dump_path)
    return user


def create_interval_dataset(dataset, look_back):
    """
    :param dataset: input array of time intervals
    :param look_back: each training set feature length
    :return: convert an array of values into a dataset matrix.
    """
    dataX, dataY = [], []
    for i in range(len(dataset) - look_back):
        dataX.append(dataset[i:i + look_back])
        dataY.append(dataset[i + look_back])
    return np.asarray(dataX), np.asarray(dataY)


def get_train_test_set_04(start_date, end_date):
    dump_path = './cache/get_train_test_set_04_%s_%s.pkl' % (start_date, end_date)
    if is_exist(dump_path):
        all_order = load_data(dump_path)
    else:
        all_order = get_all_order()
        all_order = all_order[(all_order['o_date'] > start_date) & (all_order['o_date'] < end_date)]
        all_order = all_order.sort_values(by='o_date', ascending=False)
        # o_date_one_hot = pd.get_dummies(all_order['o_date'])
        all_order = all_order[['o_area', 'o_sku_num', 'user_id']]
        # all_order = pd.concat([o_date_one_hot, all_order], axis=1)
        dump_data(all_order, dump_path)

    user = all_order[['user_id']]
    del all_order['user_id']
    return user, all_order,


def get_user_by_sex(sex):
    dump_path = './cache/get_user_by_sex_%s' % (sex)
    if is_exist(dump_path):
        user = load_data(dump_path)
    else:
        user = get_all_user()
        user = user[user['sex'] == sex]
        dump_data(user, dump_path)
    return user


def get_user_by_age(age):
    dump_path = './cache/get_user_by_sex_%s' % (age)
    if is_exist(dump_path):
        user = load_data(dump_path)
    else:
        user = get_all_user()
        user = user[user['age'] == age]
        dump_data(user, dump_path)
    return user


def get_on_day_order(day):
    dump_path = './cache/get_on_day_order_%s.pkl' % (day)
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        order = order[order['o_date'] == day]
        dump_data(order, dump_path)
    return order


if __name__ == '__main__':
    start_date = '2016-04-30'
    for i in range(365):
        date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        date = date.strftime('%Y-%m-%d')
        order_by_date = get_on_day_order(date)
        start_date = date
    # get_train_test_set_04('2016-04-30', '2017-05-01')

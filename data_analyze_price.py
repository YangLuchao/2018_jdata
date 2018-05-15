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
                  数据分析脚本，分析类品之间的关系
"""
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
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm

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


def get_all_order():
    '''
    获取所有订单信息
    :return:
    '''
    dump_path = './cache/get_all_order.pkl'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        user = get_all_user()
        order = get_from_fname(JDATA_USER_ORDER)
        order = pd.merge(order, user, how='left', on='user_id')
        dump_data(order, dump_path)
    return order


def get_order_by_date():
    '''
    通过开始时间与结束时间截取订单片段,只需要品类和销量
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/get_order_by_date_%s.pkl'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        cate = get_all_cate()
        order = pd.merge(order, cate, on='sku_id', how='left')
        order = order[['cate', 'o_sku_num']]
        order = order.groupby(['cate'], as_index=False).sum()
        dump_data(order, dump_path)
    return order


def get_order_by_date_cate_top1():
    '''
    通过开始时间与结束时间截取订单片段,只需要品类和销量,分性别维度
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/get_order_by_date_cate_top1.pkl'
    if is_exist(dump_path):
        order_cate_top1 = load_data(dump_path)
    else:
        order = get_all_order()
        cate = get_all_cate()
        order = pd.merge(order, cate, on='sku_id', how='left')
        order = order[['o_sku_num', 'sex', 'age', 'sku_id']]
        order = order.groupby(['sex', 'age', 'sku_id'], as_index=False).sum()
        order = order.groupby(['sex', 'age', 'sku_id'], as_index=False).max()
        order = pd.merge(order, cate, on='sku_id', how='left')
        order_cate_top1 = order[['sex', 'age', 'cate', 'o_sku_num']]
        order_cate_top1 = order_cate_top1.groupby(['sex', 'age', 'cate'], as_index=False).max()
        dump_data(order_cate_top1, dump_path)
    return order_cate_top1


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
        sku = sku.groupby(['cate', 'sku_id'], as_index=False).sum()
        sku = sku[['cate', 'sku_id']]
        dump_data(sku, dump_path)
    return sku


def get_all_cate():
    '''
    获取所有类别
    :return:
    '''
    dump_path = './cache/get_all_cate.pkl'
    if is_exist(dump_path):
        sku_cate = load_data(dump_path)
    else:
        sku = get_all_sku()
        sku_cate = sku[['sku_id', 'cate']]
        dump_data(sku_cate, dump_path)
    return sku_cate


def get_order_cate():
    dump_path = './cache/get_order_cate.pkl'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        cate = get_all_cate()
        order = pd.merge(order, cate, on='sku_id', how='left')
        dump_data(order, dump_path)
    return order


def get_analyze_by_date(date):
    analyze = get_order_cate()
    analyze = analyze[analyze['o_date'] == start_date]
    return analyze


def plat_date_cate_top1(order):
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['axes.unicode_minus'] = False
    X = order[['sex']]
    Y = order[['age']]
    Z = order[['o_sku_num']]
    F = order[['cate']]
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    C = []
    for f in F['cate']:
        if f == 1:
            C.append("r")
        elif f == 30:
            C.append("k")
        elif f == 46:
            C.append('y')
        elif f == 71:
            C.append('m')
        elif f == 83:
            C.append('b')
        elif f == 101:
            C.append('g')
    # C = DataFrame(C)
    ax.scatter(X['sex'], Y['age'], Z['o_sku_num'], c=C, alpha=0.4, s=10)
    ax.set_xlabel('年龄维度', fontsize=12)
    ax.set_ylabel('性别维度', fontsize=12)
    ax.set_zlabel('销量', fontsize=12)
    plt.show()


def show_analyze_date_cate_top1():
    order_by_cate_top1 = get_order_by_date_cate_top1()
    plat_date_cate_top1(order_by_cate_top1)


if __name__ == '__main__':
    # show_analyze()
    start_date = '2017-04-01'
    end_date = '2017-04-30'
    # get_order_cate()
    # get_analyze_by_date(start_date, end_date)
    # show_analyze_date()
    show_analyze_date_cate_top1()

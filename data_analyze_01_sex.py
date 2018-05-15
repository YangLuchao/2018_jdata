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


def fill_date(order_data, date):
    '''
    填充数据
    :param order_data:
    :return:
    '''
    item_1 = [1, 0, date]
    item_30 = [30, 0, date]
    item_46 = [46, 0, date]
    item_71 = [71, 0, date]
    item_83 = [83, 0, date]
    item_101 = [101, 0, date]
    if order_data.empty:
        order_data.append(item_1)
        order_data.append(item_30)
        order_data.append(item_46)
        order_data.append(item_71)
        order_data.append(item_83)
        order_data.append(item_101)
    if order_data[order_data['cate'] == 1] is None:
        order_data.append(item_1)
    if order_data[order_data['cate'] == 30] is None:
        order_data.append(item_30)
    if order_data[order_data['cate'] == 46] is None:
        order_data.append(item_46)
    if order_data[order_data['cate'] == 71] is None:
        order_data.append(item_71)
    if order_data[order_data['cate'] == 83] is None:
        order_data.append(item_83)
    if order_data[order_data['cate'] == 101] is None:
        order_data.append(item_101)
    return order_data


def get_order_by_date(date):
    '''
    通过开始时间与结束时间截取订单片段,只需要品类和销量
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/get_order_by_date_%s.pkl' % (date)
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        cate = get_all_cate()
        order = order[order.o_date == date]
        order = pd.merge(order, cate, on='sku_id', how='left')
        order = order[['cate', 'o_sku_num']]
        order = order.groupby(['cate'], as_index=False).sum()
        order['o_date'] = date
        order = fill_date(order, date)
        dump_data(order, dump_path)
    return order


def get_order_by_date_sex(date):
    '''
    通过开始时间与结束时间截取订单片段,只需要品类和销量,分性别维度
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/get_order_by_date_sex_%s.pkl' % (date)
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_all_order()
        cate = get_all_cate()
        order = order[order.o_date == date]
        order = pd.merge(order, cate, on='sku_id', how='left')
        order = order[['cate', 'o_sku_num', 'sex']]
        order = order.groupby(['cate', 'sex'], as_index=False).sum()
        order['date'] = date
        order = fill_date(order, date)
        dump_data(order, dump_path)
    return order


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


def plat_date(all_order):
    cate_1 = []
    cate_30 = []
    cate_46 = []
    cate_71 = []
    cate_83 = []
    cate_101 = []
    for i, data in enumerate(all_order):
        time.append(data['o_date'][0])
        cate_1.append(data[data['cate'] == 1]['o_sku_num'][0])
        cate_30.append(data[data['cate'] == 30]['o_sku_num'][1])
        cate_46.append(data[data['cate'] == 46]['o_sku_num'][2])
        cate_71.append(data[data['cate'] == 71]['o_sku_num'][3])
        cate_83.append(data[data['cate'] == 83]['o_sku_num'][4])
        cate_101.append(data[data['cate'] == 101]['o_sku_num'][5])
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['axes.unicode_minus'] = False
    plt.figure(facecolor='w', figsize=(20, 20))
    plt.plot(cate_1, 'r-', linewidth=1, label='1')
    plt.plot(cate_30, 'g-', linewidth=1, label='30')
    plt.plot(cate_46, 'b-', linewidth=1, label='46')
    plt.plot(cate_71, 'k-', linewidth=1, label='71')
    plt.plot(cate_83, 'm-', linewidth=1, label='83')
    plt.plot(cate_101, 'y-', linewidth=1, label='101')
    plt.title('销量对比', fontsize=18)
    plt.grid(b=True, ls=':')
    plt.show()


def plat_date_sex(all_order):
    sex_0 = []
    sex_1 = []
    sex_2 = []
    for i, data in enumerate(all_order):
        sex_0.append(data[data['sex'] == 0][['o_sku_num', 'cate']])
        sex_1.append(data[data['sex'] == 1][['o_sku_num', 'cate']])
        sex_2.append(data[data['sex'] == 2][['o_sku_num', 'cate']])
    all_data = (sex_0, sex_1, sex_2)
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['axes.unicode_minus'] = False
    plt.figure(figsize=(20, 20), facecolor='w')
    for j, data in enumerate(all_data):
        cate1 = []
        cate30 = []
        cate46 = []
        cate71 = []
        cate83 = []
        cate101 = []
        plt.subplot(3, 1, j + 1)
        for i, data in enumerate(data):
            try:
                cate_1 = data[data['cate'] == 1]
                cate_1_dic = cate_1['o_sku_num'].values
                cate_1_v = cate_1_dic[0]
                cate1.append(cate_1_v)
            except IndexError:
                cate1.append(0)

            try:
                cate_30 = data[data['cate'] == 30]
                cate_30_dic = cate_30['o_sku_num'].values
                cate_30_v = cate_30_dic[0]
                cate30.append(cate_30_v)
            except IndexError:
                cate30.append(0)

            try:
                cate_46 = data[data['cate'] == 46]
                cate_46_dic = cate_46['o_sku_num'].values
                cate_46_v = cate_46_dic[0]
                cate46.append(cate_46_v)
            except IndexError:
                cate46.append(0)

            try:
                cate_71 = data[data['cate'] == 71]
                cate_71_dic = cate_71['o_sku_num'].values
                cate_71_v = cate_71_dic[0]
                cate71.append(cate_71_v)
            except IndexError:
                cate71.append(0)

            try:
                cate_83 = data[data['cate'] == 83]
                cate_83_dic = cate_83['o_sku_num'].values
                cate_83_v = cate_83_dic[0]
                cate83.append(cate_83_v)
            except IndexError:
                cate83.append(0)

            try:
                cate_101 = data[data['cate'] == 83]
                cate_101_dic = cate_101['o_sku_num'].values
                cate_101_v = cate_101_dic[0]
                cate101.append(cate_101_v)
            except IndexError:
                cate101.append(0)

        plt.plot(cate1, 'r-', linewidth=1, label='1')
        plt.plot(cate30, 'g-', linewidth=1, label='30')
        plt.plot(cate46, 'b-', linewidth=1, label='46')
        plt.plot(cate71, 'k-', linewidth=1, label='71')
        plt.plot(cate83, 'm-', linewidth=1, label='83')
        plt.plot(cate101, 'y-', linewidth=1, label='101')

        plt.xlabel('性别 %s 的类别销量对比' % (j), fontsize=12)
        plt.ylabel('销量', fontsize=12)

    plt.grid(b=True, ls=':')
    plt.show()


def show_analyze_date():
    all_date = []
    for i in range(364):
        date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        date = date.strftime('%Y-%m-%d')
        order_by_date = get_order_by_date(date)
        all_date.append(order_by_date)
        start_date = date
    plat_date(all_date)


def show_analyze_date_sex():
    dump_path = './cache/show_analyze_date_sex.pkl'
    start_date = '2016-05-01'
    all_date = []
    for i in range(364):
        date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        date = date.strftime('%Y-%m-%d')
        order_by_date = get_order_by_date_sex(date)
        all_date.append(order_by_date)
        start_date = date
    plat_date_sex(all_date)


if __name__ == '__main__':
    # show_analyze()
    start_date = '2017-04-01'
    end_date = '2017-04-30'
    # get_order_cate()
    # get_analyze_by_date(start_date, end_date)
    # show_analyze_date()
    show_analyze_date_sex()

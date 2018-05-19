#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2018/5/18
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


def get_all_sku_06():
    '''
    获取所有商品信息
    :return:
    '''
    dump_path = './cache/get_all_sku_06.pkl'
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


def get_all_user_06():
    '''
    获取所有用户信息
    :return:
    '''
    dump_path = './cache/get_all_user_06.pkl'
    if is_exist(dump_path):
        user = load_data(dump_path)
    else:
        user = get_from_fname(JDATA_USER_BASIC_INFO)
        dump_data(user, dump_path)
    return user


def get_all_order_06():
    '''
    获取所有订单信息
    :return:
    '''
    dump_path = './cache/get_all_order_06.pkl'
    if is_exist(dump_path):
        order = load_data(dump_path)
    else:
        order = get_from_fname(JDATA_USER_ORDER)
        user = get_all_user_06()
        sku = get_all_sku_06()
        order = pd.merge(order, user, how='left', on='user_id')
        order = pd.merge(order, sku, how='left', on='sku_id')
        order = order[(order['age'] != 1) & (order['age'] != 6)]
        order = order[(order['cate'] == 101) & (order['cate'] != 71) & (order['cate'] != 30)]
        order['a_type'] = 3
        order['a_num'] = 1
        order.rename(columns={'o_date': 'date'}, inplace=True)
        order.rename(columns={'o_sku_num': 'num'}, inplace=True)
        order = order[['user_id', 'date', 'a_type', 'sku_id', 'num']]
        print(order.shape)
        dump_data(order, dump_path)
    return order


def get_all_action_06():
    '''
   将下单行为追加到行为表中,获取全部行为信息
   :return:
   '''
    dump_path = './cache/get_all_action_06.pkl'
    if is_exist(dump_path):
        actions = load_data(dump_path)
    else:
        actions = get_from_fname(JDATA_USER_ACTION)
        sku = get_all_sku_06()
        user = get_all_user_06()
        actions = actions.groupby(['user_id', 'a_date', 'a_type', 'sku_id'], as_index=False).sum()
        actions = pd.merge(actions, sku, how='left', on='sku_id')
        actions = pd.merge(actions, user, how='left', on='user_id')
        actions = actions[(actions['age'] != 1) & (actions['age'] != 6)]
        actions = actions[(actions['cate'] == 101) & (actions['cate'] != 71) & (actions['cate'] != 30)]
        actions.rename(columns={'a_date': 'date'}, inplace=True)
        actions.rename(columns={'a_num': 'num'}, inplace=True)
        actions = actions[['user_id', 'date', 'a_type', 'sku_id', 'num']]
        print(actions.shape)
        dump_data(actions, dump_path)
    return actions


def get_filter_order(order, cate=None, para_2=None, para_3=None, age=None, sex=None, user_lv_cd=None):
    '''
    获取过滤后的订单
    :param cate:101,30,71
    :param para_2:-1,1,2,3,4,5,6
    :param para_3:-1,1,2,3,4,5,6,7
    :param age:1,1,2,3,4,5,6
    :param sex:0,1,2
    :param user_lv_cd:1,2,3,4,5
    :return:
    '''
    order = get_all_order_06()
    if cate:
        for d in cate:
            order = order[order['cate'] == d]
    if para_2:
        for d in para_2:
            order = order[order['para_2'] == d]
    if para_3:
        for d in para_3:
            order = order[order['para_3'] == d]
    if age:
        for d in age:
            order = order[order['age'] == d]
    if sex:
        for d in sex:
            order = order[order['sex'] == d]
    if user_lv_cd:
        for d in user_lv_cd:
            order = order[order['user_lv_cd'] == d]
    return order


def get_order_by_cate_age_sex():
    pass


def get_all_action_order_06():
    user = get_all_user_06()
    sku = get_all_sku_06()
    action = get_all_action_06()
    order = get_all_order_06()
    action_order = pd.concat([action, order], axis=0)
    action_order = pd.merge(action_order, sku, how='left', on='sku_id')
    action_order = pd.merge(action_order, user, how='left', on='user_id')
    # action_order = action_order[(action_order['age'] != 1) & (action_order['age'] != 6)]
    # action_order = action_order[
    #     (action_order['cate'] == 101) & (action_order['cate'] != 71) & (action_order['cate'] != 30)]
    action_order = action_order.sort_values(by='date', ascending=True)
    return action_order


def get_train_test_set(step_size=7, cate=None, para_2=None, para_3=None, age=None, sex=None, user_lv_cd=None):
    action_order = get_all_action_order_06()
    action_order = get_filter_order(action_order, cate=None, para_2=None, para_3=None, age=None, sex=None,
                                    user_lv_cd=None)

    pass


def get_handle_order_06():
    # order_age_2 = get_filter_order(cate=101, age=2)
    # order_age_3 = get_filter_order(cate=101, age=3)
    # order_age_4 = get_filter_order(cate=101, age=4)
    # order = pd.concat([order_age_2, order_age_3, order_age_4], axis=0)
    # order = order.sort_values(by='o_date', ascending=False)
    # print(len(order))
    # actions = get_all_action_06()
    order = get_all_order_06()
    order = order.groupby(['o_date', 'cate'], as_index=False).mean()
    order = order[order['cate'] == 101]
    del order['user_id']
    del order['sku_id']
    del order['o_id']
    del order['o_date']
    del order['cate']
    return order


if __name__ == '__main__':
    get_train_test_set_06()

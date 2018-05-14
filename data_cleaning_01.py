#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2018/5/8
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
                  数据清洗第一版，封装方法，理清滑动窗口思路
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
        del order['o_area']
        del order['o_sku_num']
        dump_data(order, dump_path)
    return order


def get_all_action():
    '''
   将下单行为追加到行为表中,获取全部行为信息
   :return:
   '''
    dump_path = './cache/get_all_action.pkl'
    dump_path_2 = './cache/get_all_action_2.pkl'
    if is_exist(dump_path):
        actions = load_data(dump_path)
    else:
        if is_exist(dump_path_2):
            actions = load_data(dump_path_2)
        else:
            actions = get_from_fname(JDATA_USER_ACTION)
            dump_data(actions, dump_path_2)
        orders = get_all_order()
        orders['a_num'] = 1
        orders['a_type'] = 3
        del orders['o_id']
        orders.rename(columns={'o_date': 'a_date'}, inplace=True)
        actions = pd.concat([actions, orders], axis=0)
        dump_data(actions, dump_path)

    return actions


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


def handle_comment_date(comment):
    '''
    评论时间格式化
    :param comment:
    :return:
    '''
    comment['comment_create_tm'] = comment['comment_create_tm'].split(' ')[0]
    return comment


def get_all_comment():
    '''
    获取所有商品评论信息
    :return:
    '''
    dump_path = './cache/get_all_comment.pkl'
    if is_exist(dump_path):
        comment = load_data(dump_path)
    else:
        comment = get_from_fname(JDATA_USER_COMMENT_SCORE)
        score_level_one_hot = pd.get_dummies(comment['score_level'], prefix='score_level')
        comment = comment.apply(handle_comment_date, axis=1)
        comment = pd.concat([comment, score_level_one_hot], axis=1)
        del comment['score_level']
        dump_data(comment, dump_path)
    return comment


def get_comment_order():
    '''
    获取所有商品评论信息并关联订单信息
    :return:
    '''
    dump_path = './cache/get_comment_order.pkl'
    if is_exist(dump_path):
        comment = load_data(dump_path)
    else:
        order = get_all_order()
        comment = get_all_comment()
        comment = pd.merge(comment, order, on=['user_id', 'o_id'], how='left')
    return comment


def get_comment_by_date(start_date):
    '''
    通过时间截取并统计评论信息
    :param start_date:
    :param end_start:
    :return:
    '''
    dump_path = './cache/get_comment_by_date_%s.pkl' % (start_date)
    if is_exist(dump_path):
        comment = load_data(dump_path)
    else:
        comment = get_comment_order()
        comment = comment[comment.comment_create_tm < start_date]
        del comment['user_id']
        del comment['o_id']
        del comment['comment_create_tm']
        del comment['o_date']
        comment = comment.groupby(['sku_id'], as_index=False).sum()
        comment.rename(
            columns={'score_level_1': 'score_level_1_%s' % start_date, 'score_level_2': 'score_level_2_%s' % start_date,
                     'score_level_3': 'score_level_3_%s' % start_date}, inplace=True)
        dump_data(comment, dump_path)
    return comment


def get_actions_by_date(start_date, end_date):
    '''
    通过开始于结束时间获取行为
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/get_actions_by_date_%s_%s.pkl' % (start_date, end_date)
    if is_exist(dump_path):
        actions = load_data(dump_path)
    else:
        actions = get_all_action()
        actions = actions[(actions.a_date >= start_date) & (actions.a_date < end_date)]
        dump_data(actions, dump_path)
    return actions


def get_action_left_join_user_sku_comment_by_date(start_date, end_date):
    '''
    action left join user sku comment by date
    :return:
    '''
    dump_path = './cache/get_action_left_join_user_sku_comment_%s_%s.pkl' % (start_date, end_date)
    if is_exist(dump_path):
        action_user_sku_comment = load_data(dump_path)
    else:
        action_user_sku = get_actions_by_date(start_date, end_date)
        comment = get_comment_by_date(start_date)
        del action_user_sku['a_date']
        action_user_sku = action_user_sku.groupby(['user_id', 'sku_id', 'a_type'], as_index=False).sum()
        a_type_one_hot = pd.get_dummies(action_user_sku['a_type'], prefix='a_type')
        action_user_sku = pd.concat([action_user_sku, a_type_one_hot], axis=1)
        del action_user_sku['a_type']
        action_user_sku_comment = pd.merge(action_user_sku, comment, on='sku_id', how='left')
        action_user_sku_comment = action_user_sku_comment.fillna(0)
        action_user_sku_comment.rename(columns={'a_num': 'a_num_%s_%s' % (start_date, end_date)}, inplace=True)
        dump_data(action_user_sku_comment, dump_path)
    return action_user_sku_comment


def get_labels(start_date, end_date):
    '''
    打标签
    :param start_date:
    :param end_date:
    :return:
    '''
    dump_path = './cache/labels_%s_%s.pkl' % (start_date, end_date)
    if is_exist(dump_path):
        actions = load_data(dump_path)
    else:
        actions = get_action_left_join_user_sku_comment_by_date(start_date, end_date)
        # 购买行为的用户信息
        actions = actions[actions['a_type_3'] == 1]
        actions = actions.groupby(['user_id', 'sku_id'], as_index=False).mean()
        actions['label'] = 1
        actions = actions[['user_id', 'sku_id', 'label']]
        dump_data(actions, dump_path)
    return actions


def get_train_test_set(o_time):
    '''
    获取训练和测试数据
    :return:
    '''
    date_interval = [1, 7, 14, 21, 30, 60, 90, 180, 270, 360]
    dump_path = './cache/get_train_test_set_%s.pkl' % (o_time)

    user = get_all_user()
    sku = get_all_sku()
    actions = None
    if is_exist(dump_path):
        actions = load_data(dump_path)
    else:
        for i in reversed(date_interval):
            # 确定结束时间，处理结束时间
            start_days = datetime.strptime(o_time, '%Y-%m-%d') - timedelta(days=i)
            start_days = start_days.strftime('%Y-%m-%d')
            dump_date_2 = './cache/get_train_test_set_%s_%s.pkl' % (start_days, o_time)
            if is_exist(dump_date_2):
                if actions is None:
                    actions = load_data(dump_date_2)
                else:
                    action = load_data(dump_date_2)
                    actions = pd.merge(actions, action, how='left',
                                       on=['user_id', 'sku_id', 'a_type_1', 'a_type_2', 'a_type_3'])
                    actions = actions.fillna(0)
            else:
                actions = get_action_left_join_user_sku_comment_by_date(start_days, o_time)
                dump_data(actions, dump_date_2)

    actions = pd.merge(actions, user, on=['user_id'], how='left')
    actions = pd.merge(actions, sku, on=['sku_id'], how='left')
    user_sku = actions['user_id', 'sku_id'].copy()

    del actions['user_id']
    del actions['sku_id']

    return user_sku, actions


if __name__ == '__main__':
    start_date = '2017-04-12'
    end_date = '2017-04-14'
    get_train_test_set('2017-04-30')
    # get_all_action()
    # get_all_user()
    # get_all_sku()
    # get_all_order()
    # get_all_comment()
    # get_comment_by_date(end_date)
    # get_actions(start_time, end_time)
    # get_order_by_date(start_time, end_time)
    # get_comment_by_date(start_date, end_date)
    # get_action_left_join_user_sku_comment_lable_by_date(start_date, end_date)

#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from collections import Counter
import pandas as pd
import numpy as np
import datetime
import copy
import xgboost as xgb
from sklearn.model_selection import train_test_split
from tqdm import tqdm
import warnings

warnings.filterwarnings('ignore')

sku = pd.read_csv('./data_ori/jdata_sku_basic_info.csv', )
action = pd.read_csv('./data_ori/jdata_user_action.csv', parse_dates=['a_date'])
basic_info = pd.read_csv('./data_ori/jdata_user_basic_info.csv')
comment_score = pd.read_csv('./data_ori/jdata_user_comment_score.csv', parse_dates=['comment_create_tm'])
order = pd.read_csv('./data_ori/jdata_user_order.csv', parse_dates=['o_date'])

order = pd.merge(order, sku, on='sku_id', how='left')
# print(order)
order = pd.merge(order, basic_info, on='user_id', how='left')
# print(order)
action = pd.merge(action, sku, how='left', on='sku_id')
# print(action)
order['month'] = order['o_date'].apply(lambda x: x.month)
action['month'] = action['a_date'].apply(lambda x: x.month)

train_month = 4
# train_action = action[action['month'] != 4]
# # print(train_action)


# # 构建训练集：是首次购买日期，所以训练集只取训练月份最早购买的那一天
train_data = order[order['month'] == train_month][['user_id', 'o_date', 'cate','price','para_1','para_2','para_3']].sort_values(by=['user_id', 'o_date']).drop_duplicates()
train_data = train_data.drop(train_data[train_data[['user_id', 'cate']].duplicated()].index, axis=0)
train_data = pd.merge(train_data, basic_info, on='user_id', how='left')
train_data['day']=train_data['o_date'].apply(lambda x: x.day)
# today = datetime.datetime(2017, train_month, 16)
# first_day=datetime.datetime(2017, train_month, 1)
# day=today-first_day
# print(day)

# print(first_day)
ITEM_TABLE_FILE = "item_table.csv"
train_data.to_csv(ITEM_TABLE_FILE, index=False)


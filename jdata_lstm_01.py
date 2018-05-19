#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2018/5/16
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

import pandas as pd
import numpy as np
import random
import data_cleaning_06 as cl
from keras.models import Sequential, model_from_json
from keras.layers import Dense, LSTM, Dropout
from data_cleaning_04 import get_train_test_set_04
from sklearn.preprocessing import Normalizer
from sklearn.model_selection import train_test_split
from matplotlib import pyplot


def RNN():
    order = cl.get_train_test_set_06()
    sc = Normalizer()  # scaling using normalisation
    order = sc.fit_transform(order)
    Z = order[:, 1]
    Y = []
    for i, z in enumerate(Z):
        if i % 5 == 0:
            Y.append(z)
    X = np.delete(order, 1, 1)
    X = np.reshape(X, (73, 5, 21))

    model = Sequential()
    model.add(LSTM(1, return_sequences=False,
                   input_shape=(5, 21)))  # returns a sequence of vectors of dimension 32
    # model.add(LSTM(7, return_sequences=True))  # returns a sequence of vectors of dimension 32
    # model.add(Dropout(0.5))
    # model.add(LSTM(1))  # return a single vector of dimension 32
    model.add(Dense(1, activation='linear'))
    model.compile(loss='mean_squared_error',
                  optimizer='adam',
                  metrics=['accuracy'])
    history = model.fit(X, Y, batch_size=7, epochs=50, validation_split=0.3, shuffle=False)

    pyplot.plot(history.history['loss'], label='train')
    pyplot.plot(history.history['val_loss'], label='test')
    pyplot.legend()
    pyplot.show()


if __name__ == '__main__':
    RNN()

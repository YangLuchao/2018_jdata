#coding=utf-8
from __future__ import print_function
import os
import pandas as pd
from keras.layers import Input, Dense, Dropout, Conv2D, MaxPooling2D, Flatten
from keras.models import Model, load_model  
from keras.callbacks import ModelCheckpoint 
from sklearn.preprocessing import MinMaxScaler
import cv2, numpy as np

training_set=pd.read_csv('item_table.csv')   #reading csv file
training_set_x=training_set.iloc[:,2:10].values
# 将整型变为float
dataset_x = training_set_x.astype('float32')


scaler_x = MinMaxScaler ( feature_range =( 0, 1))
x = scaler_x.fit_transform (dataset_x)    

model = load_model("./my_model.h5")
model.compile(loss='categorical_crossentropy',
            optimizer='rmsprop',
            metrics=['accuracy'])
print(x.shape)
# print(x[0].reshape(1,9))
pred=model.predict(x[2].reshape(1,8))
print(pred*30)
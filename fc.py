import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense,Dropout
from keras.layers import LSTM
training_set=pd.read_csv('item_table.csv')   #reading csv file
print(training_set.head())			   #print first five rows

training_set_x=training_set.iloc[:,2:10].values
training_set_y=training_set.iloc[:,10:11].values
# 将整型变为float
dataset_x = training_set_x.astype('float32')
dataset_y = training_set_y.astype('float32')

# print(dataset)
# sc = MinMaxScaler()			   #scaling using normalisation 
# training_set1 = sc.fit_transform(dataset)
# print(training_set1)

scaler_x = MinMaxScaler ( feature_range =( 0, 1))
x = scaler_x.fit_transform (dataset_x)
 
 
scaler_y =MinMaxScaler ( feature_range =( 0, 1))
y = scaler_y.fit_transform (dataset_y)

train_end=int(len(y)*0.7)

# x_train = x [0: train_end,]
# x_test = x[ train_end +1:len(x),]    
# y_train = y [0: train_end] 
# y_test = y[ train_end +1:len(y)] 

model = Sequential ()
# model.add (LSTM (1000 , activation = 'tanh', inner_activation = 'hard_sigmoid' , input_shape =(x.shape[1], 1) ))
# model.add(Dropout(0.2))
model.add(Dense(32, input_shape=(x.shape[1],)))
model.add(Dense(64,activation='tanh'))
model.add (Dense(1, activation = 'linear'))
print(model.summary())
model.compile (loss ="mean_squared_error" , optimizer = "adam")   
model.fit (x, y, batch_size =16, nb_epoch =25, validation_split=0.3,shuffle = False)
model.save('my_model.h5')   
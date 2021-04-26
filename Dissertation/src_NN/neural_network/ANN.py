import itertools
import keras
#import matplotlib
#matplotlib.use('GTK3Agg')
#import matplotlib.pyplot as plt
#from matplotlib import pyplot as plt
#import tkinter

#%matplotlib inline
import numpy as np
import os
import tensorflow as tf
#from networkx.drawing.tests.test_pylab import plt

tf.random.set_seed(100)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler, OneHotEncoder
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
#import openpyxl
from keras.utils import np_utils

#from IPython.display import display
from numpy.random import seed
import pandas as pd
seed(888)
from sklearn.metrics import confusion_matrix


# Load the data from GCP
#cha_data_raw = pd.read_csv("gs://basic_company_data_csvs/edited_basic_company_data_2020_12_01_part1_6.csv")
cha_data_raw = pd.read_csv("gs://basic_company_data/700_sample_per_class.csv") #("gs://basic_company_data/diss_basic_comp_data.csv")
# Create a copy
cha_data = cha_data_raw.copy()
print(cha_data)

# Check the data
cha_data.info()

# check for nulls
print(cha_data.notnull())

# Drop NaNs if there are any
cha_data = cha_data.dropna(how='any')
print(cha_data.isna().sum())

# Transform the categorical data - these are the classes
lab_enc = LabelEncoder()
encoded_data = lab_enc.fit_transform(cha_data['CompanyCategory'])
encoded_data.shape

enc = pd.get_dummies(cha_data['name'])

# X will be the data i.e. tags and y the classes
X = enc
#X = cha_data.iloc[:, 0].values #1:-1].values
y = encoded_data

# Creating an 80% train and 20% test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=8)

#X_train = X_train.reshape(X_train.shape[0])
#X_test = X_test.reshape(X_test.shape[0])

#X_train = X_train.to_numpy()
#tags_array1 = tags_array.reshape(-1, 1)
#X_test = X_test.to_numpy()


# Scaling the data
scaler = StandardScaler()
X_train_scal = scaler.fit_transform(X_train)
X_test_scal = scaler.fit_transform(X_test)

print(f'X_train is after scaler {X_train.shape}')
print(f'X_test is after scaler {X_test.shape}')

# Creating the validation set
validation_size = 560
X_val_set = X_train[:validation_size]
y_val_set = y_train[:validation_size]

# Creates numpy arrays of X_train and y_train - do I need this?
X_train = X_train[validation_size:]
y_train = y_train[validation_size:]

# Converts a class vector (int) to binary class matrix
y_train = np_utils.to_categorical(y_train, 4) #5)
y_test = np_utils.to_categorical(y_test, 4) #5)

#print(f'the shape of X_train = {X_train.shape}')
#print(f'the shape of y_train before = {y_train.shape}')

#X_train = np.asarray(X_train).reshape(-1, 1)
#y_train = np.asarray(y_train).reshape(-1, 1)
#y_test = np.asarray(y_test).reshape(-1, 1)

print(f'the shape of model X_train = {X_train.shape}')
print(f'the shape of model y_train = {y_train.shape}')

#X_train_rav = X_train.ravel()
#print(f'the shape of X_train after ravel= {X_train_rav.shape}')

#y_train_rav = y_train.ravel()
#print(f'the shape of y_train after ravel= {y_train_rav.shape}')


# Creating the structure of the Neural Network
# We have n input features and x classes
# (n + x) /2

# Structuring an initialising the NN
nnetwork = Sequential()
#nnetwork.add(Dropout(0.2, seed=42, input_shape=(total_input,)))
nnetwork.add(Dense(input_shape=(314,), units=32, activation='relu')) # Need to change N
#nnetwork.add(Dropout(0.1))
nnetwork.add(Dense(units=16, activation='relu', name='hidden_layer_1'))
nnetwork.add(Dense(units=8, activation='relu', name='hidden_layer_2'))
nnetwork.add(Dense(units=4, activation='softmax', name='output_layer'))

# Compiling the ANN
nnetwork.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy']) #'sparse_catergorical_crossentropy', 'adamax'
nnetwork.fit(X_train, y_train, epochs=24)
print(nnetwork.summary())

#num_epochs = 5
#batch_size = 10
#nnetwork.fit(X_train, y_train, epochs=num_epochs, validation_data=(X_val_set, y_val_set)) # batch_size
#nnetwork1

#print(nnetwork.history.keys())

plt.plot(nnetwork.history.history['loss'])
plt.title('Loss')
#plt.savefig("diss.png")
plt.show()

# Evaluating the algorithm
test_accuracy = nnetwork.evaluate(X_test, y_test)

prediction = nnetwork.predict(X_test)
prediction[0]

y_test[0]

#print(cha_data['name'][np.argmax(prediction[0])])

#Testing the model.
test_loss, test_accuracy = nnetwork.evaluate(X_test, y_test)
print(f'The test loss is {test_loss: 0.5} and the test accuracy is {test_accuracy: 0.1%}')
"""
plt.plot(nnetwork1.history['accuracy'])
plt.title('Accuracy')
plt.show()

#Testing the model.
test_loss, test_accuracy = nnetwork.evaluate(X_test, y_test)
print(f'The test loss is {X_test: 0.5} and the test accuracy is {y_test: 0.1%}')
"""



'''
Input is taken to be a .csv file with 3 columns, first having the initial text, 
second having the secondary text which is in reply, 3rd having the sentiment or 
relation between them as is to be found in the discourse analysis.

filePath is to be set. It is the path of the dataset (.csv file)

dense_unitNo is the number of different outputs that we have.

Checkpointing is used as a fallback mechanism. 

'''
# importing libs
import pandas as pd
import matplotlib.pyplot as pyplot
import numpy as np
import datetime
from keras.model import Sequential
from keras.layers import Dense, LSTM, Dropout
from keras.callbacks import TensorBoard, ModelCheckpoint


# assigning filepath of dataset
filePath = ""

# assigning filename of the model that is to be saved, saved as __________.h5 
modelName = ""

# assigning LSTM unit no
unitNo = 256

# assigning dense unit no (NO OF OUTPUTS)
dense_unitNo = 0

# importing dataset
dataset_train = pd.read_csv(filePath)
training_set = dataset_train

# datastruct for training
posts_train = training_set.drop(training_set[-1], axis=1, inplace=False)
labels_train = training_set[-1]


# -------------------RNN------------------

# initialising
model = Sequential()

# Adding first LSTM layer and Dropout regularisation
model.add(LSTM(units=unitNo, return_sequences=True, input_shape=(1, 2)))
model.add(Dropout(0.2))

# Adding second layer and regularisation
model.add(LSTM(units=unitNo, return_sequences=True))
model.add(Dropout(0.2))

# Adding third layer and regularisation
model.add(LSTM(units=unitNo, return_sequences=True))
model.add(Dropout(0.2))

# Adding fourth layer and regularisation
model.add(LSTM(units=unitNo, return_sequences=True))
model.add(Dense(64, activation="relu", input_dim=3))
model.add(Dropout(0.2))

# Adding output layer
model.add(Dense(units=dense_unitNo, activation="softmax"))

# compiling RNN
model.compile(
    optimizer="rmsprop", loss="categorical_crossentropy", metrics=["accuracy"]
)

#setting up checkpoint logs and tensorboard
logdir =  "logs/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
tb_callback = TensorBoard(log_dir=logdir)
ckpt_filepath = ""
chkpt_callback = ModelCheckpoint(ckpt_filepath, save_weights_only=True, verbose=1)

# fitting the return_sequences
model.fit(posts_train, labels_train, epochs=300, batch_size=128, callbacks=[tb_callback, chkpt_callback])

model.save(f'{modelName}.h5')

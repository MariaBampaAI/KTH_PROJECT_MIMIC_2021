''' Recurrent Neural Network in Keras for use on the MIMIC-III '''
import gc
from time import time
import os
import math
import pickle

import numpy as np
import pandas as pd

#from keras import backend as K
from keras.models import Model, Input, load_model #model_from_json
from keras.layers import Masking, Flatten, Embedding, Dense, LSTM, TimeDistributed
from keras.callbacks import TensorBoard, ModelCheckpoint
from keras.preprocessing.sequence import pad_sequences
from keras import regularizers
from keras import optimizers


from sklearn.preprocessing import RobustScaler, MinMaxScaler
from sklearn.metrics import confusion_matrix, accuracy_score, roc_auc_score, classification_report
from sklearn.metrics import recall_score, precision_score
from sklearn.model_selection import StratifiedKFold



######################################
## MAIN ###
######################################

def pad(df, lb, time_steps, pad_value=-100):

    ''' Takes a file path for the dataframe to operate on. lb is a lower bound to discard 
        ub is an upper bound to truncate on. All entries are padded to their ubber bound '''

    #df = pd.read_csv(path):
    uniques = pd.unique(df['hadm_id'])
    df = df.groupby('hadm_id').filter(lambda group: len(group) > lb).reset_index(drop=True)
    df = df.groupby('hadm_id').apply(lambda group: group[0:time_steps]).reset_index(drop=True)
    df = df.groupby('hadm_id').apply(lambda group: pd.concat([group, pd.DataFrame(pad_value*np.ones((time_steps-len(group), len(df.columns))), columns=df.columns)], axis=0)).reset_index(drop=True)
         
    return df



def delete_columns(df):
    COLUMNS = list(df.columns)
    COLUMNS.remove('mortality')

    if  'Unnamed: 0' in COLUMNS:
        COLUMNS.remove('Unnamed: 0')
    if 'hadm_id' in COLUMNS:
        COLUMNS.remove('hadm_id')
    if 'subject_id' in COLUMNS:
        COLUMNS.remove('subject_id')
    if 'counts' in COLUMNS:
        COLUMNS.remove('counts')
    return COLUMNS


def ZScoreNormalize(matrix):

    ''' Performs Z Score Normalization for 3rd order tensors 
            matrix should be (batchsize, time_steps, features) 
            Padded time steps should be masked with np.nan '''

    x_matrix = matrix[:,:,0:-1]
    y_matrix = matrix[:,:,-1]
    print(y_matrix.shape)
    y_matrix = y_matrix.reshape(y_matrix.shape[0],y_matrix.shape[1],1)
    means = np.nanmean(x_matrix, axis=(0,1))
    stds = np.nanstd(x_matrix, axis=(0,1))
    print(x_matrix.shape)
    print(means.shape)
    print(stds.shape)
    x_matrix = x_matrix-means
    print(x_matrix.shape)
    x_matrix = x_matrix / stds
    print(x_matrix.shape)
    print(y_matrix.shape)
    matrix = np.concatenate([x_matrix, y_matrix], axis=2)
        
    return matrix
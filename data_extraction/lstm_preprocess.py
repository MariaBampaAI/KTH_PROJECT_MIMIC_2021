''' Recurrent Neural Network in Keras for use on the MIMIC-III '''
import gc
from time import time
import os
import math
import pickle

import numpy as np
import pandas as pd

from keras.preprocessing.sequence import pad_sequences




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


def normalize(X_train, X_test, X_val):

    #make sure they are double or single format
    train = np.double(X_train)
    val = np.double(X_val)
    test = np.double(X_test)
    # Compute the mean and standard deviation vector for the training data and then normalize the training, 
    # validation and test data w.r.t.  these mean and standard deviation vectors.
    #del X_test, X_train, X_val
    train_mean = np.mean(train, axis=(0,1)) 
    train_std = np.std(train, axis=(0,1))
    #just checking the shapes
    print("Shape should be dx1: ", train_mean.shape)

    train -= train_mean
    train /= train_std 

    val -= train_mean
    val /= train_std 

    test -= train_mean
    test /= train_std 


    return train, val, test
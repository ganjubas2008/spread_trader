import numpy as np
import pandas as pd
from datetime import tzinfo, timedelta, datetime
import matplotlib.pyplot as plt
import ast
from tqdm import tqdm
from copy import deepcopy
import os

def read_df(name, paths, nrows=100_000):
    df = None
    if nrows == None:
        df = pd.read_csv(paths[name], low_memory=False)  # Process in chunks of 10,000 rows
    else:
        df = pd.read_csv(paths[name], nrows=nrows, low_memory=False)  # Process in chunks of 10,000 rows

    df = df.sample(frac=0.1, random_state=42)
    
    df = df.rename(columns={col: i for i, col in enumerate(df.columns)})
    
    df = df[df[4] == 0]
    df = df.reset_index().drop(['index'], axis=1)

    df[2] = pd.to_datetime(df[2])
    df[3] = pd.to_datetime(df[3])

    #df.drop([0, 1], axis=1, inplace=True)
    
    return df

import pandas as pd


def colunaStrEmData(df,coluna):
    for i in coluna:
        df[f'{i}'] = pd.to_datetime(df[f'{i}'])
    
    
    return df
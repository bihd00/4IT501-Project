import pandas as pd
import numpy as np
import re

def clean_returns(df):
    df1 = df.drop(['Order ID', 'Product ID', 'Sub-Category', 'Manufacturer','Product Name','Return Reason','Notes'],axis=1)
    filter_df = df1[df1['Order Date'].apply(lambda x: True if re.search('^[A-Za-z]{2}-[0-9]{4}-[0-9]{6}', x) else False)]
    unique_df = filter_df.drop_duplicates(subset='Order Date')
    cleaned_df = unique_df.rename(columns={"Row ID": "isReturned", "Order Date" : "Order_ID"})
    cleaned_df['isReturned'] = cleaned_df['isReturned'].astype('bool')
    return cleaned_df

def clean_quotas(df):
    cleaned_df = df.drop(['Unnamed: 5'], axis=1)
    cleaned_df = cleaned_df.dropna()
    cleaned_df.columns = cleaned_df.iloc[0]
    cleaned_df = cleaned_df[1:]
    return cleaned_df

def union_sales(data_2020,data_2021,data_2022,data_2023):
    union_data = pd.concat([data_2020, data_2021, data_2022, data_2023])
    return union_data
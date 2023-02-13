# -*- coding: utf-8 -*-
# @Time    : 09-06-2022 16:08
# @Author  : sachin
# @Email   : spb722@gmail.com
# @File    : util.py.py
# @Software: PyCharm
import pandas as pd
from sklearn.cluster import KMeans


def convert_types(df, col: list, datatype: str):
    try:
        if not isinstance(col, list):
            raise ValueError("col is not a list")

        if datatype not in ['int', 'float', 'str', 'int64', 'float64', 'datetime64[ns]']:
            raise ValueError("data type provided is not correcdt")

        for name in col:
            df[name] = df[name].astype(datatype)
            print(f"converted {name} to ---- {datatype}")
        return df
    except Exception as e:
        print("eror occourend in convert types", e)
        raise ValueError(e)


def kmean_cluster(data, cluster_number: int, col: list, cluster_name='cluster'):
    df = data.copy()

    try:
        kmeans = KMeans(n_clusters=cluster_number)
        kmeans.fit(df[col])
        df[cluster_name] = kmeans.predict(df[col])
        return df
    except Exception as e:
        print("error occoured in kmean_cluster", e)
        raise ValueError(e)


def order_cluster(cluster_field_name, target_field_name, df, ascending):
    df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
    df_new = df_new.sort_values(by=target_field_name, ascending=ascending).reset_index(drop=True)
    df_new['index'] = df_new.index
    df_final = pd.merge(df, df_new[[cluster_field_name, 'index']], on=cluster_field_name)
    df_final = df_final.drop([cluster_field_name], axis=1)
    df_final = df_final.rename(columns={"index": cluster_field_name})
    return df_final

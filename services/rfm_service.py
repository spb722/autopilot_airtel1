# -*- coding: utf-8 -*-
# @Time    : 09-06-2022 14:50
# @Author  : sachin
# @Email   : spb722@gmail.com
# @File    : rfm_service.py.py
# @Software: PyCharm
import pandas as pd
import numpy as np
import os
from common.util import *
from fastapi import Depends, FastAPI, HTTPException
import pathlib
import dask.dataframe as dd
import sql_app.schemas as schemas
import datetime
import dask.array as da
import configuration.config as cfg
import configuration.features as f
from pathlib import Path
from icecream import ic
import  traceback
generic_dict = {"numerical_col": "Revenue"}
config = cfg.Config().to_json()
features = f.Features().to_json()

#ic.configureOutput(outputFunction=lambda msg: cfg.append(msg, config.log_file))

# ----------------------------------------------------------------------------------
def standarScaling(df1, features, allFeatures=False):
    df = df1.copy()
    scaler = StandardScaler()
    try:
        if allFeatures == True:
            print("the boolean alfeaters is true", allFeatures)
            scaler.fit(df)
            df.loc[:, :] = scaler.transform(df)
            return df

        scaler.fit(df[features].values.reshape(-1, 1))
        print("the boolean alfeaters is false", allFeatures)
        df[features] = scaler.transform(df[features].values.reshape(-1, 1))

    except Exception as e:
        print("exception occoured in standarScaling", e)

    return df


def find_elbow(K, distortions):
    kn = KneeLocator(K, distortions, curve='convex', direction='decreasing')
    elbow_point = kn.knee
    print("the elbow_point is ", elbow_point)
    return elbow_point


def k_mean_clustering(df2, df, elbow_point):
    kmeanModel = KMeans(n_clusters=3)
    kmeanModel.fit(df2[generic_dict.get("numerical_col")].values.reshape(-1, 1))
    df['Cluster'] = kmeanModel.predict(df2[generic_dict.get("numerical_col")].values.reshape(-1, 1))
    return df


def dump_kmean_cluster(df):
    n_cluster = df.Cluster.unique()
    for i in range(len(n_cluster)):
        fileName = "/log/magikuser/autopilot_data/delete_test_path/Cluster_" + str(i) + ".csv"
        df[df.Cluster == i].to_csv(fileName, header=True, index=False)
        # df1[df1.Cluster==i].to_csv('')
        print(n_cluster[i])


def findElbowAndPlot(df, revenue_columns):
    if revenue_columns == None:
        revenue_columns = df.columns

    distortions = []
    inertias = []
    mapping1 = {}
    mapping2 = {}
    K = range(1, 10)

    for k in K:
        kmeanModel = KMeans(n_clusters=k).fit(df[revenue_columns].values.reshape(-1, 1))
        kmeanModel.fit(df[revenue_columns].values.reshape(-1, 1))
        distortions.append(
            sum(np.min(cdist(df[revenue_columns].values.reshape(-1, 1), kmeanModel.cluster_centers_, 'euclidean'),
                       axis=1)) / df.shape[0])
        inertias.append(kmeanModel.inertia_)
        mapping1[k] = sum(
            np.min(cdist(df[revenue_columns].values.reshape(-1, 1), kmeanModel.cluster_centers_, 'euclidean'),
                   axis=1)) / \
                      df.shape[0]
        mapping2[k] = kmeanModel.inertia_

    return K, distortions


# ----------------------------------------------------------------------------------

#
# def create_paths(file_names, base_dir, dag_run_id):
#     if isinstance(base_dir, list):
#         for name, path in zip(file_names, base_dir):
#             pathlib.Path(os.path.join(path, dag_run_id)).mkdir(parents=True, exist_ok=True)
#
#         return
#     for name in file_names:
#         pathlib.Path(os.path.join(base_dir, dag_run_id)).mkdir(parents=True, exist_ok=True)


def read_preprocess(dag_run_id):
    """
    here reading 3 months last data and concating it to a single df
    df : holds the concated df
    output: here we are returning users transaction along with  all the msisdns
    """
    df = None

    try:
        pathlib.Path(os.path.join(cfg.purchase_location, dag_run_id)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(os.path.join(cfg.pack_info_location, dag_run_id)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(os.path.join(cfg.ml_location, dag_run_id)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(os.path.join(cfg.etl_location, dag_run_id)).mkdir(parents=True, exist_ok=True)

        etl_file_name = ["etl_m1_merge.csv", "etl_m2_merge.csv", "etl_m3_merge.csv"]
        purchase_file_name = ["purchase_m1.csv", "purchase_m2.csv", "purchase_m3.csv"]

        for etl, pur in zip(etl_file_name, purchase_file_name):
            print("etl file is ", etl)
            print("purchase file is ", pur)

            etl_df = dd.read_csv(os.path.join(cfg.source_purchase_and_etl_location, etl), usecols=cfg.columns,
                                 dtype=cfg.dtype)
            etl_df = etl_df.fillna(0)

            purchase_df = pd.read_csv(os.path.join(cfg.source_purchase_and_etl_location, pur))

            purchase_df = purchase_df[~purchase_df['product_id'].isin(cfg.dont_needed_pid)]

            purchase_df.dropna(inplace=True)

            etl_df = etl_df[etl_df['msisdn'].isin(purchase_df['msisdn'])]

            etl_df = etl_df.compute()

            purchase_df.to_csv(os.path.join(cfg.purchase_location, dag_run_id, pur), index=False, header=True)
            etl_df.to_csv(os.path.join(cfg.etl_location, dag_run_id, etl), index=False, header=True)

            data_li = [pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, name)) for name in
                       purchase_file_name]
            concat_data = pd.concat(data_li)
            concat_data.rename(columns={"purchase_date": "event_date", "product_id": "package_id",
                                        "total_product_price": "total_rvne"}, inplace=True)
            concat_data = concat_data.dropna()
            concat_data = convert_types(concat_data, ['package_id', 'total_rvne'], 'int64')
            df = concat_data.copy()

        all_columns = set(df.columns)
        if 'event_date' not in all_columns:
            raise ValueError("event date not present ")
        df['InvoiceDate'] = pd.to_datetime(df['event_date'])
        tx_user = pd.DataFrame(df['msisdn'].unique())
        tx_user.columns = ['msisdn']
        df.to_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_data.csv'), index=False, header=True)
        print("tx_data outputed")
        tx_user.to_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'), index=False, header=True)
        print("tx_user outputed")
        print("read_preprocess has been completed")

    except Exception as e:
        print("error occoureds in read and process rfm ", e)
        # return schemas.BaseResponse(statusCode=400, message="error occoureds in read and process rfm" + str(e),
        #                             status='error')

        raise HTTPException(status_code=400, detail="error occoureds in read and process rfm" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def find_recency(dag_run_id):
    try:

        if not os.path.exists(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv')):
            raise ValueError("tx_user and data not outputed please run preprocess first")

        tx_data = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_data.csv'))
        tx_user = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'))
        print("laoded data for rescency ")
        tx_max_purchase = tx_data.groupby('msisdn').InvoiceDate.max().reset_index()

        tx_max_purchase.columns = ['msisdn', 'MaxPurchaseDate']
        tx_max_purchase = convert_types(df=tx_max_purchase, col=['MaxPurchaseDate'], datatype='datetime64[ns]')
        tx_max_purchase['Recency'] = (
                tx_max_purchase['MaxPurchaseDate'].max() - tx_max_purchase['MaxPurchaseDate']).dt.days
        tx_user = pd.merge(tx_user, tx_max_purchase[['msisdn', 'Recency']], on='msisdn')
        # clustering----------------------#
        print("going to perform cluster")

        tx_user = kmean_cluster(data=tx_user, col=['Recency'], cluster_number=5, cluster_name="RecencyCluster")
        tx_user = order_cluster('RecencyCluster', 'Recency', tx_user, True)

        print("done with clustering and gpby")
        tx_user.to_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'), index=False, header=True)
        print("tx_user outputed  in find_recency")
    except Exception as e:
        print("the error occoured in find_recency ")
        # return schemas.BaseResponse(statusCode=400, message="error occoureds in find_recency" + str(e), status='error')
        raise HTTPException(status_code=400, detail="error occoureds in find_recency" + str(e))
    return schemas.BaseResponse(statusCode=200, message="recency sucessfull", status="success")


def find_frequency(dag_run_id):
    try:

        tx_data = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_data.csv'))
        tx_user = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'))
        tx_frequency = tx_data.groupby('msisdn').InvoiceDate.count().reset_index()
        tx_frequency.columns = ['msisdn', 'Frequency']
        tx_user = pd.merge(tx_user, tx_frequency, on='msisdn')

        tx_user = kmean_cluster(data=tx_user, col=['Frequency'], cluster_number=5, cluster_name="FrequencyCluster")
        print("done with clustering")
        tx_user = order_cluster('FrequencyCluster', 'Frequency', tx_user, True)
        tx_user.to_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'), index=False, header=True)
        print("tx_user outputed  in find_frequency")
        pass
    except Exception as e:
        print("the error occoured in find_frequency ")
        # return schemas.BaseResponse(statusCode=400, message="error occoureds in find_frequency" + str(e),
        #                           status='error')
        raise HTTPException(status_code=400, detail="error occoureds in find_frequency" + str(e))
    return schemas.BaseResponse(statusCode=200, message="frequency sucessful", status="success")


def find_monetary(dag_run_id):
    try:
        tx_data = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_data.csv'))
        tx_user = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'))

        tx_max_purchase = convert_types(df=tx_data, col=['InvoiceDate'], datatype='datetime64[ns]')
        tx_data.rename(columns={'total_rvne': 'Revenue'}, inplace=True)
        tx_revenue = tx_data.groupby('msisdn').Revenue.sum().reset_index()
        tx_user = pd.merge(tx_user, tx_revenue, on='msisdn')

        tx_user = kmean_cluster(data=tx_user, col=['Revenue'], cluster_number=5, cluster_name="RevenueCluster")
        tx_user = order_cluster('RevenueCluster', 'Revenue', tx_user, True)
        tx_user.to_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'), index=False, header=True)
        print("tx_user outputed  in find_monetary")
        pass
    except Exception as e:
        print("the error occoured in find_monetary ")
        # return schemas.BaseResponse(statusCode=400, message="error occoureds in find_monetary" + str(e), status='error')
        raise HTTPException(status_code=400, detail="error occoureds in find_monetary" + str(e))
    return schemas.BaseResponse(statusCode=200, message="monitory sucessfulll", status="success")


def clustering(dag_run_id):
    try:

        tx_user = pd.read_csv(os.path.join(cfg.purchase_location, dag_run_id, 'tx_user.csv'))
        tx_user['OverallScore'] = tx_user['RecencyCluster'] + tx_user['FrequencyCluster'] + tx_user['RevenueCluster']
        tx_user.groupby('OverallScore')['Recency', 'Frequency', 'Revenue'].mean()
        tx_user['Segment'] = 'Low-Value'
        tx_user.loc[tx_user['OverallScore'] > 2, 'Segment'] = 'Mid-Value'
        tx_user.loc[tx_user['OverallScore'] > 4, 'Segment'] = 'High-Value'

        for val in ['Low-Value', 'Mid-Value', 'High-Value']:
            data = tx_user[tx_user['Segment'] == val]
            path = f"{os.path.join(cfg.purchase_location, dag_run_id, val)}.csv"
            data.to_csv(path, header=True, index=False)
    except Exception as e:
        print("error occoured in final clustering", e)
        # return schemas.BaseResponse(statusCode=400, message="clustering" + str(e), status='error')
        raise HTTPException(status_code=400, detail="error occoureds in clustering" + str(e))

    return schemas.BaseResponse(statusCode=200, message="clustering successful", status="success")


def kkpp():
    try:
        pass
    except Exception as e:
        print("error occoured in final clustering", e)
        # return schemas.BaseResponse(statusCode=400, message="clustering" + str(e), status='error')
        raise HTTPException(status_code=400, detail="error occoureds in clustering" + str(e))
    return schemas.BaseResponse(statusCode=200, message="clustering successful", status="success")


def join_rfm(x):
    return str(int(x['R_Score'])) + str(int(x['F_Score'])) + str(int(x['M_Score']))


def perform_rfm(df_data_rfm, period=60):

    ic("inside perform_rfm")

    df_data_rfm.purchase_date = dd.to_datetime(df_data_rfm.purchase_date)
    df_data_rfm = df_data_rfm.fillna(0)
    msisdn_name = f.Features.MSISDN_COL_NAME
    # current behaviour data
    min_date = df_data_rfm['purchase_date'].min().compute()
    cur_beh_data_rfm = df_data_rfm[(df_data_rfm.purchase_date <= pd.Timestamp(min_date) + pd.Timedelta(days=period))
                                   & (df_data_rfm.purchase_date >= pd.Timestamp(min_date))].reset_index(drop=True)
    # Get the maximum purchase date of each customer and create a dataframe with it together with the customer's id.
    ctm_max_purchase = cur_beh_data_rfm.groupby(msisdn_name).purchase_date.max().reset_index()
    ctm_max_purchase.columns = [msisdn_name, 'MaxPurchaseDate']

    # Find the recency of each customer in days
    ctm_max_purchase['Recency'] = (
            ctm_max_purchase['MaxPurchaseDate'].max() - ctm_max_purchase['MaxPurchaseDate']).dt.days
    ctm_max_purchase = ctm_max_purchase.drop(columns=['MaxPurchaseDate'])
    print("done with recency ")
    # frequency
    ctm_frequency = cur_beh_data_rfm.groupby(msisdn_name).total_package_id.sum().reset_index()
    ctm_frequency.columns = [msisdn_name, 'Frequency']
    print("done with frequency ")
    cur_beh_data_rfm['Revenue'] = cur_beh_data_rfm[f.Features.TRANSACTION_PRICE_COL_NAME]
    ctm_revenue = cur_beh_data_rfm.groupby('msisdn').Revenue.sum().reset_index()
    print("done with monitory ")
    rfm_data_base = dd.concat([ctm_max_purchase, ctm_revenue, ctm_frequency], axis=1)
    rfm_data_base = rfm_data_base.loc[:, ~rfm_data_base.columns.duplicated()]
    return rfm_data_base


def form_segements(ctm_class):
    def process_duplicates(li):
        for i in range(len(li) - 1):
            print(li[i], i)
            if i + 1 == len(li) - 1:
                li[i] = ((li[i - 1] + li[i]) / 2)
            elif li[i] == li[i + 1]:
                li[i + 1] = ((li[i + 1] + li[i + 2]) / 2)
        return li

    bins_recency = [-1,
                    np.percentile(ctm_class["Recency"], 20),
                    np.percentile(ctm_class["Recency"], 40),
                    np.percentile(ctm_class["Recency"], 60),
                    np.percentile(ctm_class["Recency"], 80),
                    ctm_class["Recency"].max().compute()]
    bins_recency.sort()
    bins_recency = process_duplicates(bins_recency)

    ctm_class['R_Score'] = ctm_class["Recency"].map_partitions(pd.cut,
                                                               bins=bins_recency,
                                                               labels=[5, 4, 3, 2, 1]).astype("int")

    bins_frequency = [-1,
                      np.percentile(ctm_class["Frequency"], 20),
                      np.percentile(ctm_class["Frequency"], 40),
                      np.percentile(ctm_class["Frequency"], 60),
                      np.percentile(ctm_class["Frequency"], 80),
                      ctm_class["Frequency"].max().compute()]

    bins_frequency.sort()
    bins_frequency = process_duplicates(bins_frequency)
    ctm_class['F_Score'] = ctm_class["Frequency"].map_partitions(pd.cut,
                                                                 bins=bins_frequency,
                                                                 labels=[1, 2, 3, 4, 5]).astype("int")

    bins_revenue = [-1,
                    np.percentile(ctm_class["Revenue"], 20),
                    np.percentile(ctm_class["Revenue"], 40),
                    np.percentile(ctm_class["Revenue"], 60),
                    np.percentile(ctm_class["Revenue"], 80),
                    ctm_class["Revenue"].max().compute()]
    bins_revenue.sort()
    bins_revenue = process_duplicates(bins_revenue)
    ctm_class['M_Score'] = ctm_class["Revenue"].map_partitions(pd.cut,
                                                               bins=bins_revenue,
                                                               labels=[1, 2, 3, 4, 5]).astype("int")
    print("done with scoring")
    # Form RFM segment

    ctm_class['RFM_Segment'] = ctm_class.apply(join_rfm, axis=1)
    ctm_class['RFM_Segment'] = ctm_class['RFM_Segment'].astype(int)
    print("formed rfm segement ")
    ctm_class['R_Score'] = ctm_class['R_Score'].astype(int)
    ctm_class['F_Score'] = ctm_class['F_Score'].astype(int)
    ctm_class['M_Score'] = ctm_class['M_Score'].astype(int)
    print("computing rfm")
    r = ctm_class['R_Score'].values.compute()
    f = ctm_class['F_Score'].values.compute()
    m = ctm_class['M_Score'].values.compute()
    seg = segmentaion_fun1(r, f, m)
    chunks = ctm_class.map_partitions(lambda x: len(x)).compute().to_numpy()
    myarray = da.from_array(seg, chunks=tuple(chunks))
    ctm_class['Segment'] = myarray
    return ctm_class


def segmentaion_fun1(r, f, m):
    # Assigning segment names to customers
    segments = []
    for r, f, m in zip(r, f, m):
        if (4 <= r <= 5) and (4 <= f <= 5) and (4 <= m <= 5):
            segments.append("Champions")
        elif (2 <= r <= 5) and (3 <= f <= 5) and (3 <= m <= 5):
            segments.append("Loyal_Customers")
        elif (3 <= r <= 5) and (1 <= f <= 3) and (1 <= m <= 3):
            segments.append("Potential_Loyalist")
        elif (4 <= r <= 5) and (f == 1) and (m == 1):
            segments.append("Recent_Customers")
        elif (3 <= r <= 4) and (f == 1) and (m == 1):
            segments.append("Promising_Customers")
        elif (2 <= r <= 3) and (2 <= f <= 3) and (2 <= m <= 3):
            segments.append("Customers_needing_Attention")
        elif (2 <= r <= 3) and (1 <= f <= 2) and (1 <= m <= 2):
            segments.append("About_to_Sleep")
        elif (1 <= r <= 2) and (2 <= f <= 5) and (2 <= m <= 5):
            segments.append("At_Risk")
        elif (r == 1) and (4 <= f <= 5) and (4 <= m <= 5):
            segments.append("Cant_Loose_them")
        elif (1 <= r <= 2) and (1 <= f <= 2) and (1 <= m <= 2):
            segments.append("Hibernating")
        elif (r == 1) and (1 <= f <= 2) and (1 <= m <= 2):
            segments.append("Hibernating")
        else:
            segments.append("Customers_needing_Attention")

    # pur['Segment'] = segments

    return segments


def rfm_process_quantile_method(dag_run_id):
    try:
        pass
        file_name_dict = cfg.get_file_names()
        dtype_purchase = f.Features.TRANSACTION_DTYPES
        data = {}
        print('purchase is going to  read')
        for month in cfg.Config.purchase_no_months:
            data[month] = dd.read_csv(
                os.path.join(cfg.Config.purchase_location, file_name_dict.get("purchase").get(month)),
                dtype=dtype_purchase)
        print('purchase readed')
        df_data = dd.concat(list(data.values()))
        df_data = df_data.fillna(0, )
        df_data = df_data.rename(columns={f.Features.TRANSACTION_PURCHASE_DATE_NAME: "purchase_date"})
        ctm_class = perform_rfm(df_data, period=60)
        path = os.path.join(cfg.Config.ml_location,dag_run_id, "rfm_before_segementation")
        Path(path).mkdir(parents=True, exist_ok=True)
        print(path)
        ctm_class.to_parquet(path)
        ctm_class = form_segements(ctm_class)
        print("done with rfm outputing the file ongoing")
        path = os.path.join(cfg.Config.ml_location,dag_run_id, "rfm")
        Path(path).mkdir(parents=True, exist_ok=True)
        print(path)
        ctm_class.to_parquet(path)

        ic("rfm segement value counts", ctm_class['Segment'].value_counts().compute())
        print("done with rfm outputing the file done ")

    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds rfm_process_quantile_method" + str(e))
    return schemas.BaseResponse(statusCode=200, message="rfm_process_quantile_method", status="success")

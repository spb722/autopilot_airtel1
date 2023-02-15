# @Filename:    feature_engineering_service.py
# @Author:      sachin
# @Time:        12-01-2023 15:52
# @Email:       spb722@gmail.com
import pandas as pd
from icecream import ic
import dask.dataframe as dd
import os
import configuration.config as cfg
import configuration.features as f
import traceback
import numpy as np
from fastapi import Depends, FastAPI, HTTPException
from pathlib import Path
import vaex
import pickle

config = cfg.Config().to_json()
features = f.Features().to_json()
import sql_app.schemas as schemas


def put_revenue_segement(x):
    y = 'Zero'
    if x == 0.0000:
        y = 'Zero'
    elif (x > 0) & (x <= 16):
        y = 'very_low'
    elif (x > 16) & (x <= 36):
        y = 'low'
    elif (x > 36) & (x <= 61):
        y = 'medium'
    elif (x > 61) & (x <= 175):
        y = 'high'
    else:
        y = 'high_high'
    return y


def trends(check):
    asc = desc = True
    i, L = 0, len(check)
    while (i < (L - 1)) and (asc or desc):
        if check[i] < check[i + 1]:
            desc = False
        elif check[i] > check[i + 1]:
            asc = False
        if not (asc or desc):
            break
        i += 1
    if asc and desc:
        return "flat"
    elif asc:
        return "Downtrend"
    elif desc:
        return "Uptrend"
    else:
        return "zig_zag"


def return_values(x, y, z):
    return [x, y, z]


def pandas_wrapper(row):
    return trends([row[0], row[1], row[2]])


def otliner_removal(df, per=0.97):
    try:
        df = df["needed_col"]
        q = df['tot_rev'].quantile(per)
        print("the length brfore is", len(df))
        df = df[df['tot_rev'] < q]
        print("the length after is", len(df))
        return df
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def arpu_trend(data):
    months = cfg.Config.usage_no_months
    temp_df = None

    for month in months:
        total_revenue = f.Features.CUSTOMER_TOTAL_REVENUE[0]
        # total_voice_rev = f.Features.CUSTOMER_VOICE_REVENUE[2]
        needed_col = [f.Features.MSISDN_COL_NAME, total_revenue]
        df = data.get(month)[needed_col]
        df['tot_rev'] = df[total_revenue]
        df = df.fillna(0)
        # df = otliner_removal(df)
        df['rev_segment'] = df.tot_rev.apply(put_revenue_segement)
        df = df.rename(columns={"rev_segment": "rev_segment_" + month})
        df['tot_rev' + month] = df['tot_rev']
        df = df.drop('tot_rev', axis=1)
        if temp_df is None:
            temp_df = df
        else:
            temp_df = temp_df.merge(df, on='msisdn', how="left")
            temp_df["rev_segment_" + month] = temp_df["rev_segment_" + month].fillna("Zero")
            temp_df["tot_rev" + month] = temp_df["tot_rev" + month].fillna(0)

    temp_df['tot_sum'] = temp_df['tot_revm1'] + temp_df['tot_revm2'] + temp_df['tot_revm3']
    temp_df_non = temp_df
    print("len of temp_df_non  after filtering is is  ", len(temp_df_non))
    replace_map = cfg.Config().get_banding_confitions().get("common")

    df1 = temp_df_non.replace(replace_map)
    print(type(temp_df_non))
    cols = df1.columns[df1.columns.str.startswith('rev_segment_')].tolist()
    for col in cols:
        df1[col] = df1[col].astype('int32')

    output = df1[cols].map_partitions(lambda part: part.apply(lambda x: pandas_wrapper(x), axis=1), meta=tuple)
    output1 = output.to_frame(name='trend')
    op = dd.concat([df1, output1], axis=1)

    return op


class UsageCategory(object):
    def __init__(self, data):
        self.data = data
        self.current_iteration_month = None
        # self.categorize()

    def voice_band(self, x, m):
        y = 'Zero'
        if x == 0.0000:
            y = 'Zero'
        elif (x > 0) & (x <= 8):
            y = '<=8'
        elif (x > 8) & (x <= 20):
            y = '8 - 20'
        elif (x > 20) & (x <= 40):
            y = '20 - 40'
        elif (x > 40) & (x <= 100):
            y = '40 - 100'
        elif (x > 100) & (x <= 200):
            y = '100 - 200'
        elif (x > 200) & (x <= 500):
            y = '200 - 500'
        elif (x > 500) & (x <= 1000):
            y = '500 - 1000'
        else:
            y = '1000+'
        return m + "_" + y

    def data_band(self, x, m):
        y = 'Zero'
        if x == 0.0000:
            y = 'Zero'
        elif (x > 0) & (x <= 50):
            y = '0-50 MB'
        elif (x > 50) & (x <= 100):
            y = '50-100 MB'
        elif (x > 100) & (x <= 250):
            y = '100-250 MB'
        elif (x > 250) & (x <= 512):
            y = '250-512 MB'
        elif (x > 512) & (x <= 1536):
            y = '512 MB-1.5 GB'
        elif (x > 1536) & (x <= 3072):
            y = '1.5-3 GB'
        else:
            y = '3 GB +'
        return m + "_" + y

    def categorize(self):
        months = ['m1', 'm2', 'm3']
        temp_df = None
        for month in months:
            self.current_iteration_month = month

            needed_col = f.Features.CUSTOMER_CATEG_NEEDED_COL
            dataset = self.data.get(month)[needed_col]
            dataset = dataset.fillna(0)
            # 0 index for inbundle 1 index for outbundled  2 index for total
            voice_col = f.Features.CUSTOMER_VOICE_COL_NAME[0]
            data_col = f.Features.CUSTOMER_DATA_COL_NAME[0]
            dataset[month + '_voice_band'] = dataset[voice_col].apply(self.voice_band, args=(month,))
            dataset[month + '_data_band'] = dataset[data_col].apply(self.data_band, args=(month,))
            dataset = dataset.drop(columns=[voice_col, data_col])
            if temp_df is None:
                temp_df = dataset
            else:
                temp_df = temp_df.merge(dataset, on='msisdn', how="left")
                temp_df[month + '_voice_band'] = temp_df[month + '_voice_band'].fillna(month + "_"'Zero')
                temp_df[month + '_data_band'] = temp_df[month + '_data_band'].fillna(month + "_"'Zero')

        return temp_df


def usage_process(dag_run_id):
    try:
        file_name_dict = cfg.get_file_names()
        print("finding trend ongoing ")
        data = {}
        for month in cfg.Config.usage_no_months:
            data[month] = dd.read_csv(os.path.join(cfg.Config.etl_location, file_name_dict.get("usage").get(month)),
                                      dtype=f.Features.CUSTOMER_DTYPES)

        ic("the length of m1 in usage ", len(data['m1']))
        ic("the length of m2 in usage ", len(data['m2']))
        ic("the length of m3 in usage ", len(data['m3']))
        ic("the length of unique msisdn m3 in usage ", data['m1']['msisdn'].nunique().compute())
        ic("the length of unique msisdn m3 in usage ", data['m2'
                                                            '']['msisdn'].nunique().compute())
        ic("the length of unique msisdn m3 in usage ", data['m3']['msisdn'].nunique().compute())

        trend_df = arpu_trend(data)

        ic("the trend value counts is ", trend_df['trend'].value_counts().compute())

        path = os.path.join(cfg.Config.ml_location, dag_run_id, "trend")
        Path(path).mkdir(parents=True, exist_ok=True)
        trend_df.to_parquet(path)
        uc = UsageCategory(data)
        path = os.path.join(cfg.Config.ml_location, dag_run_id, "usage_bands")
        print("finding usage band ongoing ")
        df = uc.categorize()
        df.to_parquet(path)
        print("finding usage band done ")

    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in read and process rfm" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


class RechargeBanding(object):
    def __init__(self, data):
        self.data = data
        # self.categorize()

    def count_category(self, x, m):

        if x == 1:
            resp = '1'
        elif 1 < x <= 4:
            resp = '1-4'
        elif 4 < x <= 10:
            resp = '4-10'
        elif 10 < x <= 30:
            resp = '10-30'
        elif x > 30:
            resp = '30 above'
        else:
            resp = 'zero'
        return m + "_" + resp

    def categorize(self):

        months = cfg.Config.recharge_no_months
        recharge_count_col_name = f.Features.RECHARGE_COUNT_COL_NAME
        msisdn_name = f.Features.MSISDN_COL_NAME
        temp_df = None
        for month in months:
            needed_col = f.Features.RECHARGE_NEEDED_COL
            dataset = self.data.get(month)[needed_col]
            dataset = dataset.groupby([msisdn_name]).agg({recharge_count_col_name: 'sum'})
            dataset['count_pattern'] = dataset[recharge_count_col_name].apply(self.count_category, args=(month,))
            dataset['recharge_count_pattern_' + month] = dataset['count_pattern']
            dataset = dataset.drop(recharge_count_col_name, axis=1)
            dataset = dataset.drop('count_pattern', axis=1)
            if temp_df is None:
                temp_df = dataset
            else:
                temp_df = temp_df.merge(dataset, on=msisdn_name, how="left")
                temp_df["recharge_count_pattern_" + month] = temp_df["recharge_count_pattern_" + month].fillna("zero")

        return temp_df.reset_index()


def recharge_process(dag_run_id):
    try:
        file_name_dict = cfg.get_file_names()
        print("rechare preprocess  ongoing ")
        data = {}
        dtype_recharge = f.Features.RECHARGE_DTYPES
        for month in cfg.Config.recharge_no_months:
            data[month] = dd.read_csv(os.path.join(cfg.Config.etl_location, file_name_dict.get("recharge").get(month)),
                                      dtype=f.Features.RECHARGE_DTYPES)

        ic("the length of m1 in recharge ", len(data['m1']))
        ic("the length of m2 in recharge ", len(data['m2']))
        ic("the length of m3 in recharge ", len(data['m3']))

        ic("the length of unique msisdn m3 in recharge ", data['m1']['msisdn'].nunique().compute())
        ic("the length of unique msisdn m3 in recharge ", data['m2']['msisdn'].nunique().compute())
        ic("the length of unique msisdn m3 in recharge ", data['m3']['msisdn'].nunique().compute())

        rb = RechargeBanding(data)
        print("recharge categorizing ongoing ")
        df = rb.categorize()
        print("recharge categorizing done ")
        path = os.path.join(cfg.Config.ml_location, dag_run_id, "recharge_band")
        Path(path).mkdir(parents=True, exist_ok=True)
        print("recharge categorizing  to file op ongoing ")
        df.to_parquet(path)
        print("recharge categorizing  to file op done  ")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in recharge_process" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


class purchaseBanding(object):
    def __init__(self, data):
        self.data = data
        # self.categorize()

    def count_category(self, x, m):

        if x == 1:
            resp = '1'
        elif 1 < x <= 4:
            resp = '1-4'
        elif 4 < x <= 10:
            resp = '4-10'
        elif 10 < x <= 30:
            resp = '10-30'
        elif x > 30:
            resp = '30 above'
        else:
            resp = 'zero'
        return m + "_" + resp

    def categorize(self):

        months = cfg.Config.purchase_no_months_seg
        purchase_count_col_name = f.Features.TRANSACTION_COUNT_COL_NAME
        msisdn_name = f.Features.MSISDN_COL_NAME
        temp_df = None
        for month in months:
            needed_col = f.Features.TRANSACTION_NEEDED_COL
            dataset = self.data.get(month)[needed_col]
            dataset = dataset.groupby([msisdn_name]).agg({purchase_count_col_name: 'sum'})
            dataset['count_pattern'] = dataset[purchase_count_col_name].apply(self.count_category, args=(month,))
            dataset['purchase_count_pattern_' + month] = dataset['count_pattern']
            dataset = dataset.drop(purchase_count_col_name, axis=1)
            dataset = dataset.drop('count_pattern', axis=1)
            if temp_df is None:
                temp_df = dataset
            else:
                temp_df = temp_df.merge(dataset, on=msisdn_name, how="left")
                temp_df["purchase_count_pattern_" + month] = temp_df["purchase_count_pattern_" + month].fillna("zero")

        return temp_df.reset_index()


def purchase_process(dag_run_id):
    try:
        file_name_dict = cfg.get_file_names()
        print("purchase preprocess  ongoing ")
        data = {}
        dtype_purchase = f.Features.TRANSACTION_DTYPES
        for month in cfg.Config.purchase_no_months_seg:
            data[month] = dd.read_csv(os.path.join(cfg.Config.etl_location, file_name_dict.get("purchase").get(month)),
                                      dtype=f.Features.TRANSACTION_DTYPES)

        ic("the length of m1 in purchase ", len(data['m1']))
        ic("the length of m2 in purchase ", len(data['m2']))
        ic("the length of m3 in purchase ", len(data['m3']))

        ic("the length of unique msisdn m3 in purchase ", data['m1']['msisdn'].nunique().compute())
        ic("the length of unique msisdn m3 in purchase ", data['m2']['msisdn'].nunique().compute())
        ic("the length of unique msisdn m3 in purchase ", data['m3']['msisdn'].nunique().compute())
        rb = purchaseBanding(data)
        print("purchase categorizing ongoing ")
        df = rb.categorize()
        print("purchase categorizing done ")
        path = os.path.join(cfg.Config.ml_location, dag_run_id, "purchase_band")
        Path(path).mkdir(parents=True, exist_ok=True)
        print("purchase categorizing  to file op ongoing ")
        df.to_parquet(path)
        print("purchase categorizing  to file op done  ")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in purchase_process" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def status_process(dag_run_id):
    try:
        trend_path = os.path.join(cfg.Config.ml_location, dag_run_id, "trend")
        trend_filtered_path = os.path.join(cfg.Config.ml_location, dag_run_id, "trend_filtered")
        trend = dd.read_parquet(trend_path)
        trend_filter = trend.query("tot_revm1 > 0 or tot_revm2 > 0 or tot_revm3 >0 ")
        trend_filter.to_parquet(trend_filtered_path)
        ic("the length of trend df ", len(trend))
        ic("the unique msisdn in trend df ", trend['msisdn'].nunique().compute())

        ic("the length of trend df  after all 3 months active ", len(trend_filter))
        ic("the unique msisdn in trend df  all 3 months active ", trend_filter['msisdn'].nunique().compute())

        pass
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in status_process" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def association_process(dag_run_id):
    try:
        pass
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in status_process" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def segment_data(recharge_trend_usage_rfm, dag_run_id):
    path_dict = {}
    try:
        for trend in recharge_trend_usage_rfm['trend'].unique():
            for segement in recharge_trend_usage_rfm['Segment'].unique():
                temp = recharge_trend_usage_rfm[
                    (recharge_trend_usage_rfm['trend'] == trend) & (recharge_trend_usage_rfm['Segment'] == segement)]

                name = f"{trend}_{segement}"
                file_name = f"{name}.csv"
                print(' file_name is ', file_name)

                length = len(temp)
                if length > 1000:
                    path = os.path.join(cfg.Config.ml_location, dag_run_id, file_name)
                    print(f"the length is suff {length} file name {name} ")
                    # print('path_dict before is' ,path_dict)

                    path_dict[str(name)] = str(path)
                    # print('path_dict after  is' ,path_dict)

                    temp.export_csv(path)
                    print('file_exported')

                else:
                    print(f"the length is  insuff {length} file name {name} ")
        # print('here')
        path_d = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        print('path_d is', path_d)
        print('path_dict is', path_dict)
        with open(path_d, 'wb') as handle:
            print('opened')
            pickle.dump(path_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
            print('path_dict dumped ')

    except Exception as e:
        print("error occoured in segment_data")
        raise Exception(e)


def segementation(dag_run_id):
    try:
        recharge_path = os.path.join(cfg.Config.ml_location, dag_run_id, "recharge_band")
        recharge = vaex.open(recharge_path, )
        print("loaded recharge")

        purchase_path = os.path.join(cfg.Config.ml_location, dag_run_id, "purchase_band")
        purchase = vaex.open(purchase_path)
        print("loaded purchase")

        trend_path = os.path.join(cfg.Config.ml_location, dag_run_id, "trend_filtered")
        trend = vaex.open(trend_path)
        print("loaded trend")
        trend = trend[['msisdn', 'rev_segment_m1', 'rev_segment_m2', 'rev_segment_m3', 'trend']]
        replace_map = cfg.Config().get_banding_confitions().get("common_reverse")

        trend['rev_segment_m1'] = trend.rev_segment_m1.map(replace_map)
        trend['rev_segment_m2'] = trend.rev_segment_m2.map(replace_map)
        trend['rev_segment_m3'] = trend.rev_segment_m3.map(replace_map)

        usage_path = os.path.join(cfg.Config.ml_location, dag_run_id, "usage_bands")
        usage = vaex.open(usage_path)
        print("loaded usage")

        rfm_path = os.path.join(cfg.Config.ml_location, dag_run_id, "rfm")
        rfm = vaex.open(rfm_path)
        rfm = rfm[['msisdn', 'Segment']]
        print("loaded rfm")
#------------nnew logic----------------#
        trend_rfm = trend.join(rfm, on='msisdn', how="inner")
        trend_rfm_recharge = trend_rfm.join(recharge, on='msisdn', how="left")
        trend_rfm_recharge['recharge_count_pattern_m1'] = trend_rfm_recharge['recharge_count_pattern_m1'].fillna("m1_0")
        trend_rfm_recharge['recharge_count_pattern_m2'] = trend_rfm_recharge['recharge_count_pattern_m2'].fillna("m2_0")
        trend_rfm_recharge['recharge_count_pattern_m3'] = trend_rfm_recharge['recharge_count_pattern_m3'].fillna("m3_0")
        trend_rfm_recharge_usage = trend_rfm_recharge.join(usage, on='msisdn', how="left")
        trend_rfm_recharge_usage_purchase = trend_rfm_recharge_usage.join(purchase, on='msisdn', how="left")
        trend_rfm_recharge_usage_purchase['recharge_count_pattern_m1'] = trend_rfm_recharge_usage_purchase[
            'recharge_count_pattern_m1'].fillna("m1_0")
        trend_rfm_recharge_usage_purchase['recharge_count_pattern_m2'] = trend_rfm_recharge_usage_purchase[
            'recharge_count_pattern_m2'].fillna("m2_0")
        trend_rfm_recharge_usage_purchase['recharge_count_pattern_m3'] = trend_rfm_recharge_usage_purchase[
            'recharge_count_pattern_m3'].fillna("m3_0")

# ------------nnew logic----------------#
#------------------------inner join logic-------------------------------#

        # recharge_trend = recharge.join(trend, on='msisdn', how="inner")
        # recharge_trend_usage = recharge_trend.join(usage, on='msisdn', how="inner")
        # ic("mergeing rechare and trend ", recharge_trend_usage['msisdn'].nunique())
        #
        # recharge_trend_usage_rfm = recharge_trend_usage.join(rfm, on='msisdn', how="inner")
        #
        # ic("mergeing rechare and trend  and rfm ", recharge_trend_usage_rfm['msisdn'].nunique())
        #
        # recharge_trend_usage_rfm_pur = recharge_trend_usage_rfm.join(purchase, on='msisdn', how="inner")
        #
        # ic("mergeing rechare and trend  and rfm  purchase", recharge_trend_usage_rfm_pur['msisdn'].nunique())
        #
        # recharge_trend_usage_rfm_pur.export_csv(
        #     os.path.join(cfg.Config.ml_location, "recharge_trend_usage_rfm_pur.csv"))
        #
        # df = recharge_trend_usage_rfm_pur.groupby(["trend", 'Segment']).agg({"msisdn": "count"})
        # df.export_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "trend_segment.csv"))
        # ------------------------inner join logic  end-------------------------------#

        segment_data(trend_rfm_recharge_usage_purchase, dag_run_id)
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in status_process" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")

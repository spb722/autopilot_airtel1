# @Author:      sachin pb
# @Time:        22-12-2022 14:54
# @Email:       spb722@gmail.com
import os.path
from pathlib import Path
from fastapi import Depends, FastAPI, HTTPException
import sql_app.schemas as schemas
from services import load_data
from configuration.features import Features
import dask.dataframe as dd
from icecream import ic
import pandas as pd
import configuration.config as cfg
from pathlib import Path
import configuration.features as f
import traceback

config = cfg.Config().to_json()
features = f.Features().to_json()


def validity_banding(df):
    validity_column = f.Features.PACK_INFO_VALIDITY_NAME
    df.loc[df[validity_column] < 1, 'band'] = 'Hourly'
    df.loc[(df[validity_column] >= 1) & (df[validity_column] < 5), 'band'] = 'Daily'
    df.loc[(df[validity_column] >= 5) & (df[validity_column] < 15), 'band'] = 'Weekly'
    df.loc[(df[validity_column] >= 15) & (df[validity_column] <= 31), 'band'] = 'Monthly'
    df.loc[df[validity_column] > 31, 'band'] = 'Unlimited'

    return df


def get_bands(dft, col, per_val=15):
    total = dft['m_count'].sum()
    li = []
    last_pointer = 0
    for i in range(0, len(dft)):
        # print("the row",i)
        # print(dft.iloc[i])
        per = (sum(dft.iloc[last_pointer:i + 1].m_count) / total) * 100
        # print("the percentage" , per)
        if per > per_val:
            band = set()
            band.add(min(dft.iloc[last_pointer:i + 1][col]))
            band.add(max(dft.iloc[last_pointer:i + 1][col]))
            # band = (min(dft.iloc[last_pointer:i+1].b_validity) , max(dft.iloc[last_pointer:i+1].b_validity))

            li.append(min(dft.iloc[last_pointer:i + 1][col]))
            last_pointer = i + 1

    band_last = (min(dft.iloc[last_pointer:][col]), max(dft.iloc[last_pointer:][col]))
    band_last = set()
    band_last.add(min(dft.iloc[last_pointer:][col]))
    band_last.add(max(dft.iloc[last_pointer:][col]))
    # print("the last band",band_last)
    li.append(min(dft.iloc[last_pointer:][col]))
    li.append(max(dft.iloc[last_pointer:][col]))
    return li


def form_bands(pur_pack_df, packinfo_df):
    data_li = []
    tot = 0
    if type(packinfo_df) == dd.core.DataFrame:
        packinfo_df = packinfo_df.compute()
    if type(pur_pack_df) == dd.core.DataFrame:
        pur_pack_df = pur_pack_df.compute()

    # pur_pack_df.to_csv('/data/autopilot/ml/pur_pack_df.csv',header=True,index=False)
    # for service in pur_pack_df['Product_Type'].unique():

    for service in ['DATA']:

        # for service in cfg.feature_mapping.keys():
        ic(f"for the serive {service}")
        pur_pack_df_service = pur_pack_df[pur_pack_df['product_type'] == service]
        # for unit_name in cfg.feature_mapping.get(service):
        for unit_name in cfg.Config.feature_unit_list:
            ic(f"for the unit name {unit_name}")
            dft = pur_pack_df_service.groupby(unit_name).agg({'msisdn': 'count'}).rename(
                columns={"msisdn": "m_count"}).reset_index()
            # remove if any categorical values are present
            dft = dft.apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()
            dft = dft.round(0)
            dft = dft.astype(int)
            print('type(dft) is ', type(dft))
            unit_count = dft.sort_values(by=unit_name)
            unit_count = unit_count.reset_index(drop=True)
            print('len of unit_count is ', len(unit_count))
            print('unit_count is ', unit_count)
            # function that return the band accourding to mininmum distribution percentage
            b = get_bands(unit_count, unit_name)
            # removing dup
            b = list(set(b))
            b.sort()
            # for cuts method is like  (0,5] means the range not including 0 to including 5 ie: x:  x>0  and x= 5
            b[0] = b[0] - 0.01
            # forming labels accourding to the bands

            cut_labels = []
            for i in range(len(b) - 1):
                cut_labels.append(f'{b[i]}-{b[i + 1]} {service} {unit_name} band')
            tot = tot + len(cut_labels)
            ic(f" the bands are {b} and the cut labels are {cut_labels}")
            # pur_pack_df_service[unit_name] = pur_pack_df_service[unit_name].apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()

            packinfo_df['band'] = pd.cut(packinfo_df[unit_name], bins=b, labels=cut_labels)
            ic("the band value_counts", packinfo_df['band'].value_counts())
            data_li.append(packinfo_df.copy())
            ic("len is ", len(data_li))
    final_df = pd.concat(data_li)
    return final_df


class PreProcessedData:
    def __init__(self, purchase=None, usage=None):
        self.purchase = purchase
        self.usage = usage
        # combined data of purchase and pack info
        self.purchase_pack_info = None


class PreprocessData:
    def __init__(self, tnm_data=None, dag_run_id=None):
        self.tnm_data = tnm_data
        self.dag_run_id = dag_run_id
        self.pre_data_obj = None
        self.pack_features_encoded = None

    def pre_process_purchase(self):
        # self.tnm_data.purchase_df['total_package_id'] = self.tnm_data.purchase_df['total_package_id'].str.lower()
        # self.tnm_data.purchase_df['total_package_id'] = self.tnm_data.purchase_df['total_package_id'].str.strip()

        final_df = self.tnm_data.purchase_df.merge(
            self.tnm_data.pack_df, left_on="product_id", right_on="product_id", how='inner')[
            ['msisdn', 'event_date', 'total_package_id', 'product_type', 'validity_in_days', 'unit_in_mb', 'price',
             'product_id']]
        final_df = final_df.dropna()

        print('len of final_df', len(final_df))
        path = os.path.join(config.get("ml_location"), self.dag_run_id, "purchase_filtered")
        print('path is ', path)
        Path(path).mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(path)

        # save the file to path

        self.pre_data_obj = PreProcessedData(final_df, None)

    def get_pre_processed_data(self):
        ic(" getting pre processed data ")
        if self.pre_data_obj is None:
            ic("error - the data is not preprocessed ")
            raise Exception("not procedssed")

        return self.pre_data_obj

    def preprocess_pack_features(self):
        #         test_df = self.pre_data_obj.purchase
        #         test_df.to_csv('/data/autopilot/ml/pre_data_obj_purchase.csv',header=True,index=False)

        #         print('len of self.pre_data_obj.purchase  ',len(self.pre_data_obj.purchase))

        df = form_bands(self.pre_data_obj.purchase, self.tnm_data.pack_df.copy())
        df1 = validity_banding(self.tnm_data.pack_df.compute())
        final_df1 = pd.concat([df, df1])
        df = pd.crosstab(final_df1[f.Features.PACK_INFO_PACK_COLUMN_NAME], final_df1["band"])
        df.columns.name = None
        df.reset_index(inplace=True)

        self.pack_features_encoded = df

    def save_pack_features_df(self, format=None):
        self.pack_features_encoded.to_csv(
            os.path.join(
                config.get("ml_location"), self.dag_run_id, 'pack_features_encoded.csv'), header=True, index=False)


class TNMData:
    def __init__(self,
                 purchase_df=None,
                 usage_df=None,
                 pack_df=None):
        self.purchase_df = purchase_df
        self.usage_df = usage_df
        self.pack_df = pack_df


def pre_process(dag_run_id):
    try:
        # loaded the data

        file_name_dict = cfg.get_file_names()
        print("purchase preprocess  ongoing ")
        data = {}
        for month in cfg.Config.purchase_no_months_seg:
            data[month] = dd.read_csv(os.path.join(cfg.Config.etl_location, file_name_dict.get("purchase").get(month)),
                                      dtype=f.Features.TRANSACTION_DTYPES)

        pack_info = dd.read_csv(os.path.join(cfg.Config.etl_location, "pack_info.csv"),
                                dtype=f.Features.PACK_INFO_DTYPES)
        tnm_data = TNMData(purchase_df=dd.concat(list(data.values())), pack_df=pack_info)
        pre_instance = PreprocessData(tnm_data, dag_run_id)
        # # merging purchase and pack
        pre_instance.pre_process_purchase()
        p_data = pre_instance.get_pre_processed_data()
        # forming user purchase count matrix
        product_name = f.Features.PACK_INFO_PACK_COLUMN_NAME
        result = (
            p_data.purchase.groupby(["msisdn", product_name])
            .agg({product_name: "count"})
            .rename(columns={product_name: "bundle_counts"})
            .reset_index()
        )
        result = result.compute()
        result.dropna(subset=[product_name], inplace=True)
        df = (
            pd.crosstab(
                result.msisdn,
                result[product_name],
                values=result.bundle_counts,
                aggfunc=sum,

            )
        )
        ic("crosstab of purchase done ")
        df1 = df.fillna(0)
        df1 = df1.reset_index()
        op_path_dir = os.path.join(cfg.Config.ml_location, dag_run_id, )
        Path(op_path_dir).mkdir(parents=True, exist_ok=True)
        df1.to_csv(os.path.join(cfg.Config.ml_location, dag_run_id, 'user_pack_matrix.csv'), header=True, index=False)
        ic("outputed user pack matrix")
        # forming pack feature matrix
        pre_instance.preprocess_pack_features()
        pre_instance.save_pack_features_df()

        ic("outputed  pack feature matrix ")
    except Exception as e:
        ic("error occoured ", e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in read and process rfm" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")

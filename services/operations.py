# @Filename:    load_data.py
# @Author:      sachin pb
# @Time:        23-12-2022 17:42
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

config = cfg.Config().to_json()
features = f.Features().to_json()
import sql_app.schemas as schemas


def matrix_operations(dag_run_id):
    try:
        matrix_pack_features = pd.read_csv(
            os.path.join(cfg.Config.ml_location, dag_run_id, "pack_features_encoded.csv"))
        matrix_user_pack = pd.read_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "user_pack_matrix.csv"))
        msisdn_list = matrix_user_pack.pop(f.Features.MSISDN_COL_NAME)
        product_ids = matrix_user_pack.columns

        product_ids = [int(x) for x in product_ids]
        matrix_pack_features = matrix_pack_features[matrix_pack_features[f.Features.PACK_INFO_PACK_COLUMN_NAME].isin(product_ids)]
        product_id_ls = matrix_pack_features.pop(f.Features.PACK_INFO_PACK_COLUMN_NAME)
        pack_features_cols = matrix_pack_features.columns
        ic("performing a x b")
        final_matrix = np.matmul(matrix_user_pack, matrix_pack_features)
        final_matrix.columns = pack_features_cols
        final_matrix.index = msisdn_list
        # normilizing the matrix
        ic("going to normalize ")
        final_matrix = final_matrix.div(final_matrix.sum(axis=1), axis=0)
        matrix_pack_features_t = matrix_pack_features.T
        final_matrix_1 = np.matmul(final_matrix.values, matrix_pack_features_t.values)
        final_matrix_1 = pd.DataFrame(final_matrix_1, index=msisdn_list, columns=product_id_ls)
        ic("got the final matrix ")
        final_matrix_1.to_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "matrix.csv"), header=True,
                              index=True)
    except Exception as e:
        ic(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in read and process rfm" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")
def matrix_operations_one(dag_run_id):
    try:
        matrix_pack_features = pd.read_csv(
            os.path.join(cfg.Config.ml_location, dag_run_id, "pack_features_encoded.csv"))
        matrix_user_pack = pd.read_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "user_pack_matrix.csv"))
        pack_info = pd.read_csv(os.path.join(cfg.Config.pack_info_location, "pack_info.csv"))
        l1 = list(pack_info[pack_info['type'] == 'addon']['product_id'].unique())
        # l2 = list(pack_info[pack_info['product_sub_type'] == 'Data_Standard']['product_id'].unique())
        l2 = [ ]
        print('l1  is ',l1 )
        print('type of l1[0]  is ',type(l1[0]) )
        needed_products = list(set(l1 + l2))
        needed_products = [int(x) for x in needed_products]
        msisdn_list = matrix_user_pack.pop("msisdn")
        product_ids = matrix_user_pack.columns
        product_ids = [int(x) for x in product_ids]
        filter_products = [str(x) for x in needed_products if x in product_ids]
        matrix_user_pack = matrix_user_pack[filter_products]
        filter_products = [int(x) for x in filter_products]
        matrix_pack_features = matrix_pack_features[matrix_pack_features['product_id'].isin(filter_products)]
        product_id_ls = matrix_pack_features.pop("product_id")
        pack_features_cols = matrix_pack_features.columns
        ic("performing a x b")
        final_matrix = np.matmul(matrix_user_pack, matrix_pack_features)
        final_matrix.columns = pack_features_cols
        final_matrix.index = msisdn_list
        # normilizing the matrix
        ic("going to normalize ")
        final_matrix = final_matrix.div(final_matrix.sum(axis=1), axis=0)
        matrix_pack_features_t = matrix_pack_features.T
        final_matrix_1 = np.matmul(final_matrix.values, matrix_pack_features_t.values)
        final_matrix_1 = pd.DataFrame(final_matrix_1, index=msisdn_list, columns=product_id_ls)
        ic("got the final matrix ")
        final_matrix_1.to_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "propensity_matrix_addon.csv"), header=True,
                            index=True)
    except Exception as e:
        ic(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in read and process rfm" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def matrix_operations_two(dag_run_id):
    try:
        matrix_pack_features = pd.read_csv(
            os.path.join(cfg.Config.ml_location, dag_run_id, "pack_features_encoded.csv"))
        matrix_user_pack = pd.read_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "user_pack_matrix.csv"))
        pack_info = pd.read_csv(os.path.join(cfg.Config.pack_info_location, "pack_info.csv"))
        l1 = list(pack_info[pack_info['type'] == 'basic']['product_id'].unique())
        # l2 = list(pack_info[pack_info['product_sub_type'] == 'Data_Standard']['product_id'].unique())
        l2 = []
        needed_products = list(set(l1 + l2))
        needed_products = [int(x) for x in needed_products]
        msisdn_list = matrix_user_pack.pop("msisdn")
        product_ids = matrix_user_pack.columns
        product_ids = [int(x) for x in product_ids]
        filter_products = [str(x) for x in needed_products if x in product_ids]
        matrix_user_pack = matrix_user_pack[filter_products]
        filter_products = [int(x) for x in filter_products]
        matrix_pack_features = matrix_pack_features[matrix_pack_features['product_id'].isin(filter_products)]
        product_id_ls = matrix_pack_features.pop("product_id")
        pack_features_cols = matrix_pack_features.columns
        ic("performing a x b")
        final_matrix = np.matmul(matrix_user_pack, matrix_pack_features)
        final_matrix.columns = pack_features_cols
        final_matrix.index = msisdn_list
        # normilizing the matrix
        ic("going to normalize ")
        final_matrix = final_matrix.div(final_matrix.sum(axis=1), axis=0)
        matrix_pack_features_t = matrix_pack_features.T
        final_matrix_1 = np.matmul(final_matrix.values, matrix_pack_features_t.values)
        final_matrix_1 = pd.DataFrame(final_matrix_1, index=msisdn_list, columns=product_id_ls)
        ic("got the final matrix ")
        final_matrix_1.to_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "propensity_matrix_basic.csv"), header=True,
                            index=True)
    except Exception as e:
        ic(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in read and process rfm" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")
# @Filename:    ml_service.py
# @Author:      sachin
# @Time:        12-01-2023 15:52
# @Email:       spb722@gmail.com
import pandas as pd
import json
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
from mlxtend.frequent_patterns import fpgrowth, association_rules
from pathlib import Path
from sql_app.repositories import AssociationRepo
import services.rule_serive as rls
import requests

config = cfg.Config().to_json()
features = f.Features().to_json()
import sql_app.schemas as schemas
from kmodes.kmodes import KModes
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.preprocessing import OneHotEncoder
from sql_app.repositories import SegementRepo

msisdn_name = f.Features.MSISDN_COL_NAME


def find_k_kmodes(X, k_range=range(1, 11), init='Huang', n_init=10):
    sse = []
    for k in k_range:
        print(k)
        kmodes = KModes(n_clusters=k, init=init, n_init=n_init).fit(X)
        sse.append(kmodes.cost_)

    elbow = k_range[sse.index(min(sse))]
    return elbow


def perform_k_modes(df, path, dag_run_id):
    try:
        # K_modes_location = os.path.join(cfg.Config.ml_location, dag_run_id, "kmodes")
        # Path(K_modes_location).mkdir(parents=True, exist_ok=True)
        objList = df.select_dtypes(include="object").columns
        X = df[objList]
        X = pd.get_dummies(X)

        # elbow = find_k_kmodes(X=X)

        km = KModes(n_clusters=2)
        km.fit(X)
        X['label'] = km.labels_
        df['label'] = km.labels_
        print(df['label'].value_counts())
        df.to_csv(path, header=True, index=False)
        print(f"outputed to path {path}")
        # for label in df['label'].unique():
        #     df_temp = df[df['label'] == label]
        #     name_path = f"{df_name}_cluster_{label}.csv"
        #     output_path = os.path.join(K_modes_location, dag_run_id, name_path)
        #     name_list[name_path] = output_path
        #     print(f"the output path is ", output_path)
        #     df_temp.to_csv(output_path, header=True, index=False)
        #     print(f"done exporting ")

        return df
    except Exception as e:
        print(e)


def k_modes(dag_run_id):
    try:

        path_d = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        path_dd = os.path.join(cfg.Config.ml_location, dag_run_id, "cluster_analysis.csv")
        with open(path_d, 'rb') as handle:
            data = pickle.load(handle)

        # needed_segements = ["Uptrend_Champions", "Uptrend_Loyal_Customers"]
        # filtered_dict = {k: v for k, v in data.items() if k in needed_segements}
        filtered_dict = {k: v for k, v in data.items()}
        data_list = []

        for item, val in filtered_dict.items():
            # val is the path item is the segment name
            df = pd.read_csv(val)
            print(f"the cluster is {item}")
            val = perform_k_modes(df, val, dag_run_id)
            val['segement_name'] = item
            data_list.append(val)

        df_final = pd.concat(data_list)
        df_final = df_final.groupby(['segement_name', 'label']).agg({"msisdn": "count"})
        df_final.to_csv(path_dd)


    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in k_modes" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def get_features(df):
    numerical_cols = df.select_dtypes(include=[np.number]).columns
    categorical_cols = df.select_dtypes(exclude=[np.number]).columns
    encoder = OneHotEncoder()
    X_cat = encoder.fit_transform(df[categorical_cols])
    selector = SelectKBest(score_func=chi2, k=10)
    selector.fit(X_cat, df['label'])
    selected_feature_indices = selector.get_support(indices=True)
    selected_features = [categorical_cols[j % len(categorical_cols)] for j in selected_feature_indices]
    importances = selector.scores_[selected_feature_indices]
    df = pd.DataFrame({'Feature': selected_features, 'Importance': importances})
    df = df.sort_values(by='Importance', ascending=False)
    return df[df['Importance'] > 0.05]['Feature'].to_list()


def write_pickle(data, path):
    with open(path, 'wb') as handle:
        print(f'opened {path} ')
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
        print(f'data dumped  {data}')


def feature_selection(dag_run_id):
    try:
        path_d = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        features_path = os.path.join(cfg.Config.ml_location, dag_run_id, "features.pickle")
        with open(path_d, 'rb') as handle:
            data = pickle.load(handle)
        # needed_segements = ["Uptrend_Champions", "Uptrend_Loyal_Customers"]
        # filtered_dict = {k: v for k, v in data.items() if k in needed_segements}
        filtered_dict = {k: v for k, v in data.items()}
        result_features = {}
        for item, val in filtered_dict.items():
            # val is the path item is the segment name
            df = pd.read_csv(val)
            features = get_features(df)
            result_features[item] = features
            print(f"the features are {features} for the segement {item}")

        write_pickle(data=result_features, path=features_path)




    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="error occoureds in k_modes" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def load_picke_file(filename):
    with open(filename, 'rb') as handle:
        data = pickle.load(handle)
    return data


def matrix_filter(dag_run_id):
    try:

        # read purchase information
        path = os.path.join(cfg.Config.ml_location, dag_run_id, "purchase_filtered")
        matrix_path = os.path.join(cfg.Config.ml_location, dag_run_id, "matrix.csv")
        matrix_basic_path = os.path.join(cfg.Config.ml_location, dag_run_id, "propensity_matrix_basic.csv")
        matrix_addon_path = os.path.join(cfg.Config.ml_location, dag_run_id, "propensity_matrix_addon.csv")
        purchase_filter_path = os.path.join(cfg.Config.ml_location, dag_run_id, "purchased_for_association")

        Path(purchase_filter_path).mkdir(parents=True, exist_ok=True)

        purchase = dd.read_parquet(path)
        path_d = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        with open(path_d, 'rb') as handle:
            data = pickle.load(handle)
        # needed_segements = ["Uptrend_Champions", "Uptrend_Loyal_Customers"]
        # filtered_dict = {k: v for k, v in data.items() if k in needed_segements}
        filtered_dict = {k: v for k, v in data.items()}
        result = {}
        for item, val in filtered_dict.items():
            # val is the path item is the segment name
            df = pd.read_csv(val)
            purchase_filter = purchase[purchase[msisdn_name].isin(df[msisdn_name])]
            # get all the unique products  in the filtered purchase
            products = purchase_filter[f.Features.TRANSACTION_PRODUCT_NAME].unique()
            purchase_list = []
            for product in products:
                purchase_filter_one_product = purchase_filter[
                    purchase_filter[f.Features.TRANSACTION_PRODUCT_NAME] == product]
                # read the mathix
                product = str(product)
                matrix_check = pd.read_csv(matrix_addon_path,nrows=2)
                if product not in  matrix_check.columns:
                        continue

                matrix = dd.read_csv(matrix_addon_path, usecols=[f.Features.MSISDN_COL_NAME, product])
                purchase_filter1 = purchase_filter_one_product.merge(matrix, on=msisdn_name, how='inner')
                purchase_filter2 = purchase_filter1[purchase_filter1[product] > cfg.Config.threshold]
                purchase_filter2 = purchase_filter2.drop(columns=[product])
                purchase_list.append(purchase_filter2)

            purchase_final = dd.concat(purchase_list)

            print('purchase_final.isnull().sum()', purchase_final.isnull().sum().compute())

            op_path = os.path.join(purchase_filter_path, item + ".csv")
            purchase_final = purchase_final.compute()
            purchase_final.to_csv(op_path)
            result[item] = op_path
            result_path = os.path.join(purchase_filter_path, 'dict.pickle')
            with open(result_path, 'wb') as handle:
                pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)

    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in k_modes" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1


class Fpgrowth(object):
    def __init__(self, purchase, dag_run_id, item):
        self.purchase = purchase
        self.frequent_itemsets = None
        self.results = None
        self.results_processed = None
        self.item_set_max_length = 4
        self.item_set_min_length = 1
        self.dag_run_id = dag_run_id
        self.item = item

        status, msg = self.form_associations()
        print(msg)

        if not status:
            return None
        self.process_association()

        self.results_processed.to_csv(os.path.join(cfg.Config.ml_location, dag_run_id, "association.csv"), header=True,
                                      index=False)
        self.results_processed.to_csv(os.path.join(cfg.Config.ml_location, dag_run_id, str(item) + "_association.csv"),
                                      header=True,
                                      index=False)

    def form_associations(self):
        print('len of purchase before fropping na ', len(self.purchase))
        self.purchase.dropna(inplace=True)
        print('len of purchase ', len(self.purchase))

        basket = (self.purchase.groupby([msisdn_name, f.Features.TRANSACTION_PRODUCT_NAME])[
                      f.Features.TRANSACTION_PRODUCT_NAME]
                  .count().unstack().reset_index().fillna(0)
                  .set_index(msisdn_name))

        print('basket created')
        print('len of basket ', len(basket))

        basket_sets = basket.applymap(encode_units)
        basket_sets_filter = basket_sets[(basket_sets > 0).sum(axis=1) >= 2]
        frequent_itemsets = fpgrowth(basket_sets_filter, min_support=0.03, use_colnames=True)
        print('frequent_itemsets created')
        print('len of frequent_itemsets ', len(frequent_itemsets))
        if frequent_itemsets is None or len(frequent_itemsets) == 0:
            # retun none so that the next cluster
            return False, "the result does not have any lenth the lenfth is " + str(len(frequent_itemsets))
        frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))

        self.frequent_itemsets = frequent_itemsets[(frequent_itemsets['length'] >= self.item_set_min_length) & (
                frequent_itemsets['length'] <= self.item_set_max_length)]
        print('frequent_itemsets filtered')
        self.results = association_rules(frequent_itemsets, metric="lift", min_threshold=2).sort_values('lift',
                                                                                                        ascending=False).reset_index(
            drop=True)
        print('association_rules created')
        print('self.results is ', self.results)
        print(' len self.results is ', len(self.results))
        print(' type self.results is ', type(self.results))
        if len(self.results) > 2:
            return True, "got the asociaitons"
        return False, "the result does not have any lenth the lenfth is " + str(len(self.results))

    def process_association(self):
        print(' inside process_association')
        results = self.results
        results['antecedents_length'] = results['antecedents'].apply(lambda x: len(x))
        results['consequents_length'] = results['consequents'].apply(lambda x: len(x))
        # antecedents1 is a set converted from frozen set antecedents
        results['antecedents1'] = results['antecedents'].apply(set)
        results['consequents1'] = results['consequents'].apply(set)

        results.drop(['antecedents', 'consequents', 'leverage', 'conviction'], inplace=True, axis=1, errors='ignore')

        self.results_processed = results


def association_process(dag_run_id, db):
    try:
        result_dict_path = os.path.join(cfg.Config.ml_location, dag_run_id, "purchased_for_association", 'dict.pickle')
        data_path = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        data_dict = load_picke_file(data_path)
        pack_info = os.path.join(cfg.Config.etl_location, 'pack_info.csv')
        pack_info_df = pd.read_csv(pack_info)
        data = load_picke_file(result_dict_path)
        # needed_segements = ["Uptrend_Champions", "Uptrend_Loyal_Customers"]
        # filtered_dict = {k: v for k, v in data.items() if k in needed_segements}
        # filtered_data__dict = {k: v for k, v in data_dict.items() if k in needed_segements}
        filtered_dict = {k: v for k, v in data.items()}
        filtered_data__dict = {k: v for k, v in data_dict.items()}

        for item, val in filtered_dict.items():
            data = pd.read_csv(filtered_data__dict.get(item))
            purchase = pd.read_csv(val)
            for cluster in data['label'].unique():
                data_temp = data[data['label'] == cluster]
                purchase_filtered = purchase[purchase[msisdn_name].isin(data_temp[msisdn_name])]

                print('len of purchase in association_process ', len(purchase_filtered))
                fp = Fpgrowth(purchase_filtered, dag_run_id, item=item)
                print( 'fp.results is ',fp.results)
                if fp.results is None or len(fp.results)==0:
                    print('skipped this iteration')
                    continue
                print('Fpgrowth completed')
                uc = UpsellCrossell(dag_run_id=dag_run_id, pack_info=pack_info_df, result=fp.results, db=db)
                uc.determine_service_type()
                if len(uc.associations_df)<=2:
                    continue
                if uc.df_cross_df is not None and len(uc.df_cross_df) > 0:
                    uc.find_crossell(segement_name=item, cluster_number=cluster)
                print("outputed crosssell files ")
                if uc.df_upsell_df is None or len(uc.df_upsell_df) == 0:
                    continue
                for number in [1, 2, 3]:
                    print('going to find upsell for number', number)
                    uc.find_upsell(type_service='upsell', anticendent_number=number, segement_name=item,
                                   cluster_number=cluster)
                print("done with upsell cross sell")

                print('UpsellCrossell completed')



    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoureds in status_process" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1

def getpackname(product_id, packinfo_df):
    product_name = "No product name"
    try:
        if (packinfo_df is not None):
            if (product_id in packinfo_df['product_id'].values):

                product_name = str(packinfo_df[packinfo_df['product_id'] == product_id].iloc[0]["product_name"])
                print("the product name of ", product_id, " is ", product_name)
            else:
                print("product_id is not in dataframe  ")
        else:
            print("some problem with the  dataframe  ")

    except Exception as e:
        print("error occurred in getpackname ", e)

    return product_name

class UpsellCrossell(object):
    def __init__(self, consequents_length=1, exclude_types=None,
                 dag_run_id=None, db=None, pack_info=None,
                 result=None, cluster_number=None, segement_name=None):
        self.segement_name_list = None
        if exclude_types is None:
            exclude_types = ['SMS']
        self.dag_run_id = dag_run_id
        self.exclude_types = exclude_types
        self.associations_df = result
        self.db = db
        self.pack_info = pack_info
        self.df_cross_df = None
        self.df_upsell_df = None
        self.consequents_length = consequents_length

        # self.read_fiels()
        self.check_files()
        # self.determine_service_type()
        # print('df_cross_df is', self.df_cross_df)
        # if self.df_cross_df is not None and len(self.df_cross_df) > 0:
        #     self.find_crossell()
        # print("outputed crosssell files ")
        # for number in [1, 2, 3]:
        #     print('going to find upsell for number', number)
        #     self.find_upsell(type_service='upsell', anticendent_number=number,)
        # print("done with upsell cross sell")
        #
        # print("outputed crosssell files ")

    def read_fiels(self):
        try:
            self.pack_info = pd.read_csv(os.path.join(cfg.Config.ml_location, 'packinfo.csv'))
            self.associations_df = pd.read_csv(os.path.join(cfg.Config.ml_location, self.dag_run_id, 'association.csv'))
        except Exception as e:
            raise ValueError(e)

    def check_files(self):
        if self.pack_info is None or self.associations_df is None:
            print(f"the packinfo or association is null")
            raise ValueError(f"the packinfo or association is null")

    def determine_service_type(self):
        try:
            print('self.associations_df is ',self.associations_df)
            print('self.associations_df.columns is ',self.associations_df.columns)
            if len(self.associations_df)<=2:
                return
            self.associations_df = self.associations_df[self.associations_df['consequents_length'] == 1]
            df = self.associations_df.apply(self.determine_service, axis=1)
            if len(df) == 0:
                return
            for type_info in self.exclude_types:
                df = df[~df['type_service'].str.contains(type_info)]

            self.df_upsell_df = df[df['service'] == 'upsell']
            self.df_cross_df = df[df['service'] != 'upsell']


        except Exception as e:
            print("error occoured in determine service" + str(e))
            raise ValueError(e)

    def determine_service(self, x):
        print('x is', x)

        group_type = f.Features.PACK_INFO_SUB_CATEGORY

        def get_pack_type(pack_name):
            type_pack = None
            print("isnide get_pack_type")
            print("pack_name is ", pack_name)
            print("group_type is ", group_type)
            # print("f.Features.PACK_INFO_PACK_COLUMN_NAME is ", f.Features.PACK_INFO_PACK_COLUMN_NAME)
            # print("self.pack_info is ", self.pack_info)
            print("len self.pack_info is ", len(self.pack_info))

            data = self.pack_info[self.pack_info[f.Features.PACK_INFO_PACK_COLUMN_NAME] == pack_name][group_type]
            print("data is ", data)
            if len(data) > 0:
                type_pack = data.values[0]
                print("type_pack is ", type_pack)
            return type_pack

        x['antecedents1'] = str(x['antecedents1'])
        x['consequents1'] = str(x['consequents1'])

        antecedents_list = list(eval(x['antecedents1']))
        print('antecedents_list is ', antecedents_list)
        consequents_list = list(eval(x['consequents1']))
        print('consequents_list is ', consequents_list)
        antecedents_length = len(antecedents_list)

        i = 0
        service = "upsell"
        type_service = None
        temp_set = set()
        try:
            # since for each antecident we need to find eg: antecident length 1 ,2 ,3
            while i < antecedents_length:
                print('i is', i)
                temp_set.add(get_pack_type(antecedents_list[i]))
                print('temp_set is', temp_set)
                i = i + 1
                print('i is', i)
            temp_set.add(get_pack_type(consequents_list[0]))
            print('temp_set 1  is', temp_set)

            if len(temp_set) > 1:
                service = "corsssell"
                type_service = ','.join(temp_set)
            elif len(temp_set) == 1:
                type_service = next(iter(temp_set))

            x['service'] = service
            x['type_service'] = type_service
            return x
        except Exception as e:
            print("error occoured in determine_service", e)
            raise ValueError(e)

    def find_crossell(self, segement_name, cluster_number):
        try:
            segement_name = f"{segement_name}-{str(int(cluster_number))}"
            # self.segement_name_list = segement_name.split("|")

            print("self.df_cross_df is ", self.df_cross_df)
            df = self.df_cross_df.apply(self.cross_sell_parser, axis=1)

            if len(df) > 0:
                df = df.sort_values(by="confidence", ascending=False)
                segements = SegementRepo.findByAutoPilotIdAndSegementName(self.db, _id=self.dag_run_id,
                                                                          segement_name=segement_name,
                                                                          cluster_number=int(cluster_number))
                if segements is None:
                    return
                self.insert_segementinfo(segements, 1, df, "crossell")
                SegementRepo.deleteById(self.db, [segements.id])
                # confidence = int(df.head(1)['confidence'].values[0] * 100)
                # current_pack = list(eval(df.head(1)['antecedents1'].values[0]))[0]
                # recommended_pack = str(df.head(1)['conci'].values[0])
                # number_of_current_packs = int(df.head(1)['antecedents_length'].values[0])
                # if confidence > .50:
                #     segements = SegementRepo.findByAutoPilotIdAndSegementName(self.db, _id=self.dag_run_id,
                #                                                               segement_name=f"{segement_name}_{str(int(cluster_number))}",
                #                                                               cluster_number=int(cluster_number))
                #     segements.campaign_type = "cross_sell"
                #     segements.confidence = confidence
                #     segements.recommended_product_name = recommended_pack
                #     segements.current_product = current_pack
                #
                #     SegementRepo.update(self.db, segements)

                # self.update_info("antecedents1", "cross_sell", segement_name, cluster_number)
            # ------------------------adding to db -------------------------------#
            for index, row in df.iterrows():
                print(row['confidence'])
                info = schemas.AssociationInfo()
                info.service_type = str(row['type_service'])
                info.type_info = str(row['val'])
                info.dag_run_id = self.dag_run_id
                info.support = round(float(row['support']), 2)
                info.confidence = round(float(row['confidence']), 2)
                info.lift = round(float(row['lift']), 2)
                print("row['conci'] is ", row['conci'])

                info.recommended_pack = str(getpackname(row['conci'],self.pack_info))
                info.recommended_pack_id = str(row['conci'])

                info.number_of_current_packs = int(row['antecedents_length'])
                info.current_pack = "|".join([str(getpackname(i,self.pack_info)) for i in list(eval(str(row['antecedents1'])))])
                info.current_pack_ids = "|".join([str(i) for i in list(eval(str(row['antecedents1'])))])

                info.segement_name = segements.segment_name
                print("added crossel info to db")
                AssociationRepo.create(db=self.db, info=info)
            # ------------------------adding to db -------------------------------#


        # ----------------------------to br looked at ------------------------------------------------------#
        # df_2 = df[df['val'] == 'crossell'].sort_values(by='confidence', ascending=False)
        #
        # df_2.drop_duplicates(subset=['consequents1'], inplace=True)
        # df_cross_conci = df_2.drop_duplicates(subset=['conci'])
        # df_cross_anti = df_2.drop_duplicates(subset=['anti'])
        # df_2 = pd.concat([df_cross_conci, df_cross_anti])
        # df_2.drop_duplicates(subset=['anti', 'conci'], keep='first')
        # # df_2.drop(['antecedent support', 'consequent support', 'support', 'lift',
        # #            'antecedents1', 'consequents1'], axis=1, inplace=True, errors='ignore')
        #
        # df_3 = df_2.merge(
        #     self.pack_info[
        #         f.Features.ALL_PACK_FEATURES].add_prefix(
        #         "ANTECEDENT_"), left_on='anti', right_on='ANTECEDENT_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
        #     how='left')
        # df_3 = df_3.merge(
        #     self.pack_info[
        #         f.Features.ALL_PACK_FEATURES].add_prefix(
        #         "CONSEQUENT_"), left_on='conci', right_on='CONSEQUENT_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
        #     how='left')
        # # print("length of df_3 before condition is  ", len(df_3))
        # # df_3 = df_3[df_3['ANTECEDENT_price'] < df_3['CONSEQUENT_price']]
        # # print("length of df_3 after condition  is  ", len(df_3))
        #
        # if os.path.exists(os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'crossell_one.csv')):
        #
        #     crossell_one_df = pd.read_csv(
        #         os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'crossell_one.csv'))
        #     df_3 = df_3.drop(['val', 'anti', 'conci'], axis=1)
        #     final_crossell_one_df = pd.concat([crossell_one_df, df_3])
        #     final_crossell_one_df.to_csv(
        #         os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'crossell_one.csv'),
        #         header=True,
        #         index=False)
        # else:
        #
        #     df_3.drop(['val', 'anti', 'conci'], axis=1, errors='ignore').to_csv(
        #         os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'crossell_one.csv'),
        #         header=True, index=False)

        # ----------------------------to br looked at ------------------------------------------------------#
        except Exception as e:
            traceback.print_exc()
            raise ValueError(e)

    def cross_sell_parser(self, x):
        cross_sell = False
        try:
            print('x is', x)
            anti = list(eval(x['antecedents1']))
            conci = list(eval(x['consequents1']))
            print('here 2 ')
            print(f"anti {anti} conci {conci}")
            if len(anti) == 1:
                anti_catagory = self.pack_info[self.pack_info[cfg.pack_name] == anti[0]]['bundle_type'].values
                anti_class = self.pack_info[self.pack_info[cfg.pack_name] == anti[0]]['bundle_type'].values
                if len(anti_catagory) > 0:
                    anti_catagory = anti_catagory[0]
                    anti_class = anti_class[0]
                conci_catagory = self.pack_info[self.pack_info[cfg.pack_name] == conci[0]]['bundle_type'].values
                conci_class = self.pack_info[self.pack_info[cfg.pack_name] == conci[0]]['bundle_type'].values
                if len(conci_catagory) > 0:
                    conci_catagory = conci_catagory[0]
                    conci_class = conci_class[0]
                print(f"the anti c  is {anti_catagory}  and conci cata is  {conci_catagory}")
                if ((anti_catagory != conci_catagory) and (anti_class != conci_class)):
                    cross_sell = True
            anti_name = None
            if len(anti) == 2:
                cross_sell = True
                for anti_item in anti:
                    print("the anti item ", anti_item)
                    anti_catagory = self.pack_info[self.pack_info[cfg.pack_name] == anti_item]['bundle_type'].values
                    conci_catagory = self.pack_info[self.pack_info[cfg.pack_name] == conci[0]]['bundle_type'].values
                    print(f"the anticent name {anti_item} conci name {conci[0]}")
                    print(f"the anticent catagory {anti_catagory} conci cata {conci_catagory}")
                    if (anti_catagory == conci_catagory):
                        cross_sell = False
                        anti_name = None
                    else:
                        anti_name = anti_item

            if cross_sell:
                x['val'] = "crossell"
                if len(anti) == 2:
                    x['anti'] = anti_name
                else:
                    x['anti'] = anti[0]

                x['conci'] = conci[0]
                return x
            x['val'] = "not_crossell"
            x['anti'] = anti[0]
            x['conci'] = conci[0]
            return x
        except Exception as e:
            print('the error occoured in find crosssell', e)
            raise ValueError(e)

    def find_upsell(self, type_service, anticendent_number, segement_name, cluster_number):
        try:

            is_upsell = 1
            if type_service != 'upsell':
                is_upsell = 0

            segements = SegementRepo.findByAutoPilotIdAndSegementName(self.db, _id=self.dag_run_id,
                                                                      segement_name=f"{segement_name}-{str(int(cluster_number))}",
                                                                      cluster_number=int(cluster_number))
            if segements is None:
                return
            if anticendent_number == 1:
                print("inside antecedent 1 ")
                df = self.df_upsell_df[self.df_upsell_df['antecedents_length'] == 1]
                data = df.sort_values(by='confidence', ascending=False)
                print(len(data.drop_duplicates(subset=['consequents1'])))
                print("data.columns inside antecedent 1 is ", data.columns)
                data1 = data.apply(self.check_upsell, anticendent_number=1, axis=1)
                print('check_upsell completed')
                # ------------------- insert db ------------------------------

                # ------------------------adding to db -------------------------------#
                for index, row in data1.iterrows():
                    print(row['confidence'])
                    info = schemas.AssociationInfo()
                    info.service_type = str(row['type_service'])
                    info.type_info = str(row['upsell_case'])
                    info.dag_run_id = self.dag_run_id
                    info.support = round(float(row['support']), 2)
                    info.confidence = round(float(row['confidence']), 2)
                    info.lift = round(float(row['lift']), 2)
                    print("row['conci'] is ", row['conci'])
                    info.recommended_pack = str(getpackname(row['conci'],self.pack_info))
                    info.recommended_pack_id = str(row['conci'])

                    info.number_of_current_packs = int(row['antecedents_length'])
                    info.current_pack = "|".join([str(getpackname(i,self.pack_info)) for i in list(eval(str(row['antecedents1'])))])
                    info.current_pack_ids = "|".join([str(i) for i in list(eval(str(row['antecedents1'])))])
                    info.segement_name = segements.segment_name
                    print('info.number_of_current_packs is', info.number_of_current_packs)
                    print('info.current_pack is', info.current_pack)
                    print('info.recommended_pack is', info.recommended_pack)

                    print('info is', info)
                    AssociationRepo.create(db=self.db, info=info)
                    print('all  inserted')

                self.insert_segementinfo(segements, anticendent_number, data1, "upsell")

                # ------------------------adding to db -------------------------------#
                # if data1 is None or len(data1) == 0:
                #     ic(f"the segement {segement_name} and cluser {cluster_number}  has no upsell 1 data1 ")
                #     return
                # ic(f"the segement {segement_name} and cluser {cluster_number} ")
                # data2 = data1[data1['is_upsell'] == is_upsell]
                # if data2 is None or len(data2) == 0:
                #     ic(f"the segement {segement_name} and cluser {cluster_number}  has no upsell 1")
                #     return
                # data2 = data2.sort_values(by='confidence', ascending=False)
                # data2 = data2.head(5)
                # segments_list = []
                # for index, row in data2.iterrows():
                #     info = schemas.SegementInfo()
                #     info.segment_name = segements.segment_name
                #     info.dag_run_id = self.dag_run_id
                #     info.current_product = str(row['antecedents1'])
                #     info.current_products_names = str(row['antecedents1'])
                #     info.recommended_product_id = str(row['conci'])
                #     info.recommended_product_name = str(row['conci'])
                #     info.predicted_arpu = None
                #     info.current_arpu = None
                #     info.segment_length = segements.segment_length
                #     info.rule = None
                #     info.actual_rule = None
                #     info.uplift = None
                #     info.incremental_revenue = None
                #     info.campaign_type = "upsell"
                #     info.campaign_name = None
                #     info.action_key = None
                #     info.robox_id = None
                #     info.samples = segements.samples
                #     info.segment_name = segements.segment_name
                #     info.current_ARPU_band = None
                #     info.current_revenue_impact = None
                #     info.customer_status = segements.customer_status
                #     info.query = segements.query
                #     info.cluster_no = segements.cluster_no
                #     info.confidence = round(float(row['confidence']), 2)
                #     segments_list.append(info)
                #
                # for segment in segments_list:
                #     SegementRepo.create(self.db, segment)
                # ------------------------------------------------------------

            # ---------------------ipo venda -----------------------------------------#
            # data2 = data1[data1['is_upsell'] == is_upsell]
            # print('here1')
            # data3 = data2.drop_duplicates(subset=['consequents1'])
            # data3['antecedent_one'] = data3['antecedents1'].apply(lambda x: list(eval(x))[0])
            # print('here2')
            # data3['consequents1'] = data3['consequents1'].apply(lambda x: list(eval(x))[0])
            # print('here3')
            # data3 = data3.merge(
            #     self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
            #         "ANTECEDENT_"),
            #     left_on='antecedent_one', right_on="ANTECEDENT_" + f.Features.PACK_INFO_PACK_COLUMN_NAME,
            #     how='left')
            # data3 = data3.merge(
            #     self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
            #         "CONSEQUENT_"),
            #     left_on='consequents1', right_on='CONSEQUENT_' + f.Features.PACK_INFO_PACK_COLUMN_NAME, how='left')
            # # data3.drop(
            # #     ['antecedent_one', 'consequents1', 'service', 'antecedent support', 'consequent support', 'lift'],
            # #     axis=1, inplace=True)
            # data4 = data3[data3['ANTECEDENT_' + f.Features.PACK_INFO_CATEGORY] == data3[
            #     'CONSEQUENT_' + f.Features.PACK_INFO_CATEGORY]]
            #
            # file_name = os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'upsell_one.csv')
            # if os.path.exists(file_name):
            #     upsell_one_df = pd.read_csv(file_name)
            #     final_upsell_one_df = pd.concat([upsell_one_df, data4])
            #     final_upsell_one_df.to_csv(file_name, header=True,
            #                                index=False)
            #
            # else:
            #
            #     data4.to_csv(os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'upsell_one.csv'),
            #                  header=True,
            #                  index=False)
            # print("outputed 1 upsell anticident ")
            # ---------------------ipo venda -----------------------------------------#

            elif anticendent_number == 2:
                print("inside antecedent 2 ")
                df = self.df_upsell_df[self.df_upsell_df['antecedents_length'] == 2]
                if len(df) == 0:
                    ic(f"the anticendent_number 2 is not present for upsell {segement_name}_{str(int(cluster_number))}")
                    return None
                data = df.sort_values(by='confidence', ascending=False)
                print(len(data.drop_duplicates(subset=['consequents1'])))
                print("data.columns inside antecedent 2  is ", data.columns)
                data1 = data.apply(self.check_upsell, anticendent_number=2, axis=1)
                # ------------------------adding to db -------------------------------#
                for index, row in data1.iterrows():
                    print(row['confidence'])
                    info = schemas.AssociationInfo()
                    info.service_type = str(row['type_service'])
                    info.type_info = str(row['upsell_case'])
                    info.dag_run_id = self.dag_run_id
                    info.support = round(float(row['support']), 2)
                    info.confidence = round(float(row['confidence']), 2)
                    info.lift = round(float(row['lift']), 2)
                    print("row['conci'] is ", row['conci'])

                    info.recommended_pack = str(getpackname(row['conci'],self.pack_info))
                    info.recommended_pack_id = str(row['conci'])

                    info.segement_name = segements.segment_name
                    info.number_of_current_packs = int(row['antecedents_length'])
                    info.current_pack = "|".join([str(getpackname(i,self.pack_info)) for i in list(eval(str(row['antecedents1'])))])
                    info.current_pack_ids = "|".join([str(i) for i in list(eval(str(row['antecedents1'])))])
                    print('info.number_of_current_packs is', info.number_of_current_packs)
                    print('info.current_pack is', info.current_pack)
                    print('info.recommended_pack is', info.recommended_pack)

                    AssociationRepo.create(db=self.db, info=info)
                # ------------------------adding to db -------------------------------#
                self.insert_segementinfo(segements, anticendent_number, data1, "upsell")
                # ---------------------ipo venda -----------------------------------------#
                # data2 = data1[data1['is_upsell'] == 1]
                # data3 = data2.drop_duplicates(subset=['consequents1'])
                # data3['antecedents_one'] = data3['antecedents1'].apply(lambda x: list(eval(x))[0])
                # data3['antecedents_two'] = data3['antecedents1'].apply(lambda x: list(eval(x))[1])
                # data3['consequent'] = data3['consequents1'].apply(lambda x: list(eval(x))[0])
                # # data3.drop(['antecedents1', 'consequents1', 'service', 'antecedent support', 'consequent support'],
                # #            axis=1,
                # #            inplace=True)
                # data3 = data3.merge(
                #     self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
                #         "ANTECEDENT_1_"),
                #     left_on='antecedents_one', right_on='ANTECEDENT_1_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
                #     how='left')
                # data3 = data3.merge(
                #     self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
                #         "ANTECEDENT_2_"),
                #     left_on='antecedents_two', right_on='ANTECEDENT_2_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
                #     how='left')
                # data3 = data3.merge(
                #     self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
                #         "CONSEQUENT_"),
                #     left_on='consequent', right_on='CONSEQUENT_' + f.Features.PACK_INFO_PACK_COLUMN_NAME, how='left')
                # file_name = os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'upsell_two.csv');
                # if os.path.exists(file_name):
                #     upsell_two_df = pd.read_csv(file_name)
                #     final_upsell_two_df = pd.concat([upsell_two_df, data3])
                #     final_upsell_two_df.to_csv(file_name, header=True,
                #                                index=False)
                #
                # else:
                #     data3.to_csv(file_name, header=True, index=False)
                # print("outputed 2 upsell anticident ")

                # ---------------------ipo venda -----------------------------------------#

            elif anticendent_number == 3:
                print("inside antecedent 3 ")
                df = self.df_upsell_df[self.df_upsell_df['antecedents_length'] == 3]
                if len(df) == 0:
                    ic(f"the anticendent_number 3 is not present for upsell {segement_name}_{str(int(cluster_number))}")
                    return None

                data = df.sort_values(by='confidence', ascending=False)
                print(len(data.drop_duplicates(subset=['consequents1'])))
                print("data.columns inside antecedent 3 is ", data.columns)
                data1 = data.apply(self.check_upsell, anticendent_number=3, axis=1)
                # ------------------------adding to db -------------------------------#
                for index, row in data1.iterrows():
                    print(row['confidence'])
                    info = schemas.AssociationInfo()
                    info.service_type = str(row['type_service'])
                    info.type_info = str(row['upsell_case'])
                    info.dag_run_id = self.dag_run_id
                    info.support = round(float(row['support']), 2)
                    info.confidence = round(float(row['confidence']), 2)
                    info.lift = round(float(row['lift']), 2)
                    print("row['conci'] is ", row['conci'])

                    info.recommended_pack = str(getpackname(row['conci'],self.pack_info))
                    info.recommended_pack_id = str(row['conci'])

                    info.segement_name = segements.segment_name
                    info.number_of_current_packs = int(row['antecedents_length'])
                    info.current_pack = "|".join([str(getpackname(i,self.pack_info)) for i in list(eval(str(row['antecedents1'])))])
                    info.current_pack_ids = "|".join([str(i) for i in list(eval(str(row['antecedents1'])))])
                    print('info.number_of_current_packs is', info.number_of_current_packs)
                    print('info.current_pack is', info.current_pack)
                    print('info.recommended_pack is', info.recommended_pack)

                    AssociationRepo.create(db=self.db, info=info)
                # ------------------------adding to db -------------------------------#
                self.insert_segementinfo(segements, anticendent_number, data1, "upsell")

            SegementRepo.deleteById(self.db, [segements.id])
            # ---------------------ipo venda -----------------------------------------#
            #     data2 = data1[data1['is_upsell'] == 1]
            #     data3 = data2.drop_duplicates(subset=['consequents1'])
            #     data3['antecedents_one'] = data3['antecedents1'].apply(lambda x: list(eval(x))[0])
            #     data3['antecedents_two'] = data3['antecedents1'].apply(lambda x: list(eval(x))[1])
            #     data3['antecedents_three'] = data3['antecedents1'].apply(lambda x: list(eval(x))[2])
            #     data3['consequent'] = data3['consequents1'].apply(lambda x: list(eval(x))[0])
            #     # data3.drop(['antecedents1', 'consequents1', 'service', 'antecedent support', 'consequent support'],
            #     #            axis=1,
            #     #            inplace=True)
            #     data3 = data3.merge(
            #         self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
            #             "ANTECEDENT_1_"),
            #         left_on='antecedents_one', right_on='ANTECEDENT_1_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
            #         how='left')
            #     data3 = data3.merge(
            #         self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
            #             "ANTECEDENT_2_"),
            #         left_on='antecedents_two', right_on='ANTECEDENT_2_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
            #         how='left')
            #     data3 = data3.merge(
            #         self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
            #             "ANTECEDENT_3_"),
            #         left_on='antecedents_three', right_on='ANTECEDENT_3_' + f.Features.PACK_INFO_PACK_COLUMN_NAME,
            #         how='left')
            #     data3 = data3.merge(
            #         self.pack_info[f.Features.ALL_PACK_FEATURES].add_prefix(
            #             "CONSEQUENT_"),
            #         left_on='consequent', right_on='CONSEQUENT_' + f.Features.PACK_INFO_PACK_COLUMN_NAME, how='left')
            #     file_name = os.path.join(cfg.Config.pack_info_location, self.dag_run_id, 'upsell_three.csv');
            #     if os.path.exists(file_name):
            #         upsell_three_df = pd.read_csv(file_name)
            #         final_upsell_three_df = pd.concat([upsell_three_df, data3])
            #         final_upsell_three_df.to_csv(file_name, header=True,
            #                                      index=False)
            #     else:
            #         data3.to_csv(file_name, header=True, index=False)
            #     print("outputed 3 upsell anticident ")
            #
            # else:
            #     raise ValueError("anticient number more that 3 ")

            # ---------------------ipo venda -----------------------------------------#
        except Exception as e:
            print("error ocoured in output service", e)
            raise ValueError(e)

    def check_upsell(self, x, anticendent_number):
        print('Inside check_upsell')

        col = f.Features.PACK_INFO_PACK_PRICE_COLUMN_NAME

        def get_price(pack_name):
            price = None
            data = self.pack_info[self.pack_info[f.Features.PACK_INFO_PACK_COLUMN_NAME] == pack_name][col]
            if (len(data) > 0):
                price = data.values[0]
            return price

        anti = None
        conci = None
        print('x is', x)
        print("x['consequents1'] is", x['consequents1'])
        print("anticendent_number is", anticendent_number)
        x['is_upsell'] = 1
        x['upsell_case'] = "correct_upsell"

        try:
            conci = list(eval(x['consequents1']))[0]
            print("teh conci is ", conci)
            conci_price = get_price(conci)
            print(f"the conci price is {conci_price}")
            x['conci'] = conci

            if anticendent_number == 1:
                anti = list(eval(x['antecedents1']))[0]
                anti_price = get_price(anti)

                if conci_price < anti_price:
                    x['is_upsell'] = 0
                    x['upsell_case'] = "notupsell"
                    x['conci'] = conci
            else:
                amti_list = list(eval(x['antecedents1']))
                for anti_obj in amti_list:

                    anti_price = get_price(anti_obj)
                    if conci_price <= anti_price:
                        x['is_upsell'] = 0
                        x['upsell_case'] = "notupsell"
                        x['conci'] = conci

            return x
        except Exception as e:
            print(" the error occoured in check_upsell ", e)
            raise ValueError(e)

    def update_info(self, anticident_name, type_of_service, segement_name, cluster_number):
        segements = SegementRepo.findByAutoPilotIdAndSegementName(self.db, segement_name=segement_name,
                                                                  cluster_number=cluster_number)
        pass

    def insert_segementinfo(self, segements, anticendent_number, data1, type_of_service):
        if data1 is None or len(data1) == 0:
            ic(f"the segement {segements.segment_name} has no upsell 1 data1 ")
            return
        if type_of_service == "upsell":
            data1 = data1[data1['is_upsell'] == 1]
            if data1 is None or len(data1) == 0:
                ic(f"the segement {segements.segment_name}  has no upsell 1")
                return
        data2 = data1.sort_values(by='confidence', ascending=False)
        data2 = data2.head(5)
        segments_list = []
        for index, row in data2.iterrows():
            rls.get_rule_query(segements)
            info = schemas.SegementInfo()
            info.segment_name = segements.segment_name
            info.dag_run_id = self.dag_run_id
            info.current_product = "|".join([str(i) for i in list(eval(str(row['antecedents1'])))])
            info.current_products_names = "|".join([str(i) for i in list(eval(str(row['antecedents1'])))])
            # if anticendent_number == 1:
            #     info.current_products_names = str(row['antecedents1'])
            # elif anticendent_number == 2:
            #     info.current_products_names = str(row['antecedents1']) + str(row['antecedents2'])
            # else:
            #     info.current_products_names = str(row['antecedents1']) + str(row['antecedents2']) + str(
            #         row['antecedents3'])
            info.recommended_product_id = str(row['conci'])
            info.recommended_product_name = str(row['conci'])
            info.predicted_arpu = None
            info.current_arpu = None
            info.segment_length = segements.segment_length
            info.rule = None
            info.actual_rule = None
            info.uplift = None
            info.incremental_revenue = None
            info.campaign_type = type_of_service
            info.campaign_name = None
            info.action_key = None
            info.robox_id = None
            info.samples = segements.samples
            info.segment_name = segements.segment_name
            info.current_ARPU_band = None
            info.current_revenue_impact = None
            info.customer_status = segements.customer_status
            info.query = segements.query
            info.cluster_no = segements.cluster_no
            info.confidence = round(float(row['confidence']), 2)
            segments_list.append(info)

        for segment in segments_list:
            SegementRepo.create(self.db, segment)


def extract_rules(df, features):
    """

    @rtype: dictonatu of cluster and ther rule 
    """
    cluster_conditions = {}
    try:
        df1 = df[features + ["label"]]
        cluster_modes = {}
        for cluster in df1['label'].unique():
            temp = df1[df1['label'] == cluster]
            cluster_df = pd.get_dummies(temp[temp.columns[temp.dtypes == 'object']], prefix_sep=":",
                                        columns=temp.columns[temp.dtypes == 'object'])
            cluster_modes[cluster] = cluster_df.mode().loc[cluster_df.mode().sum(axis=1).idxmax()].to_dict()

        # Step 3: Form the conditions for each cluster

        for cluster, modes in cluster_modes.items():
            modes = {k: v for k, v in modes.items() if v == 1}
            op_li = []
            for key in modes.keys():
                k = key.split(":")[0]
                v = key.split(":")[1]
                op = f"{k} == '{v}'"
                op_li.append(op)
            condition = " & ".join(op_li)
            # condition = ' & '.join([f"{col} == {mode}" for col, mode in modes.items() if col != 'label'])
            cluster_conditions[int(cluster)] = f"{condition}"
    except Exception as e:
        print("extract_rules :  error occoured " + str(e))
        return cluster_conditions
    return cluster_conditions


class RuleExtreaction(object):
    def __init__(self, dag_run_id=None, features_path=None, path_d=None, db=None):
        self.path_d = path_d
        self.dag_run_id = dag_run_id
        self.features_path = features_path
        self.db = db

        # going to load this varibles
        self.data = None
        self.filtered_dict = None
        self.data_feature = None
        # loading

        self.load_pickle()
        self.filter_dict()

    def load_pickle(self):
        self.data = load_picke_file(self.path_d)
        self.data_feature = load_picke_file(self.features_path)

    def filter_dict(self):
        # needed_segements = ["Uptrend_Champions", "Uptrend_Loyal_Customers"]
        # self.filtered_dict = {k: v for k, v in self.data.items() if k in needed_segements}
        self.filtered_dict = {k: v for k, v in self.data.items()}

    def execute(self):
        for item, val in self.filtered_dict.items():
            # val is the path item is the segment name
            df = pd.read_csv(val)
            print(f"the cluster is {item}")
            cluster_conditions = extract_rules(df, self.data_feature[item])
            if cluster_conditions is None:
                print(f" the segment {item} is none ,  the path is {val}")
            for cluster, rule in cluster_conditions.items():
                try:

                    info = schemas.SegementInfo()
                    # info.actual_rule
                    info.dag_run_id = self.dag_run_id
                    info.segment_name = f"{item}-{str(cluster)}"
                    info.segment_length = str(len(df))
                    info.customer_status = "active"
                    info.query = rule
                    info.cluster_no = int(cluster)
                    if len(rule) > 1:
                        info.samples = len(df.query(rule))
                    else:
                        info.samples = 0
                    SegementRepo.create(db=self.db, segement=info)
                except Exception as e:
                    print(" error occorured in rule insertion ")
                    print(e)
                    raise ValueError(e)


def rule_extraction(dag_run_id, db):
    try:
        path_d = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        features_path = os.path.join(cfg.Config.ml_location, dag_run_id, "features.pickle")
        data = load_picke_file(path_d)
        data_feature = load_picke_file(features_path)

        re = RuleExtreaction(dag_run_id=dag_run_id, db=db, features_path=features_path, path_d=path_d)
        re.execute()

        # needed_segements = ["Uptrend_Champions", "Uptrend_Loyal_Customers"]
        # filtered_dict = {k: v for k, v in data.items() if k in needed_segements}
        # for item, val in filtered_dict.items():
        #     # val is the path item is the segment name
        #     df = pd.read_csv(val)
        #     print(f"the cluster is {item}")
        #     cluster_conditions = extract_rules(df, data_feature[item])
        #     if cluster_conditions is None:
        #         print(f" the segment {item} is none ,  the path is {val}")
        #     for cluster, rule in cluster_conditions:
        #         try:
        #
        #             info = schemas.SegementInfo
        #             info.dag_run_id = dag_run_id
        #             info.segment_name = f"{item}_{str(cluster)}"
        #             info.segment_length = str(len(df))
        #             info.customer_status = "active"
        #             info.query = rule
        #             info.samples = len(df.query(rule))
        #             SegementRepo.create(db=db, segement=info)
        #         except Exception as e:
        #             print(" error occorured in rule insertion ")
        #             print(e)
        #             raise e
        #

    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoured rule_extraction" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")


def otliner_removal(df, per=0.97):
    try:
        # df = df["needed_col"]
        q = df['tot_rev'].quantile(per)
        print("the length brfore is", len(df))
        df = df[df['tot_rev'] < q]
        print("the length after is", len(df))
        return df
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def form_data(p2, df, anti_conci):
    try:
        product_id = f.Features.PACK_INFO_PACK_COLUMN_NAME
        purchase = p2[p2[product_id].isin(anti_conci)]
        pgp = purchase.copy()
        # pgp = purchase.groupby(['msisdn', 'product_id']).agg({f.Features.PA: "sum"}).reset_index()

        anti_df = pgp[pgp[product_id].isin(anti_conci[:-1])]
        conci_df = pgp[pgp[product_id] == anti_conci[-1]]

        anti_df_msisdn = anti_df[~anti_df['msisdn'].isin(conci_df['msisdn'].values)]['msisdn'].unique()
        conci_df_msisdn = conci_df[~conci_df['msisdn'].isin(anti_df['msisdn'].values)]['msisdn'].unique()
        anti_data = df[df['msisdn'].isin(anti_df_msisdn)]
        conci_data = df[df['msisdn'].isin(conci_df_msisdn)]
        print(f"the length of anti data ios {len(anti_data)} and unique is {anti_data['msisdn'].nunique()}")
        print(f"the length of conci data ios {len(conci_data)} and unique is {conci_data['msisdn'].nunique()}")
        anti_data['label'] = 1
        conci_data['label'] = 0
        data = pd.concat([anti_data, conci_data], axis=0)
        print("label counts", data['label'].value_counts())
        return data
    except Exception as e:
        print("the error occoured in form_data", e)
        raise ValueError(e)


def generate_boundries(features, data_json, tree):
    def add_conditions(feature, count):
        rule_json = {}
        rule_json['id'] = -1

        min_max = data_json.get(feature)
        if min_max.get('min') == 0:
            rule = f"{feature} <= {round(float(min_max.get('max')), 2)}"


        else:
            rule = f"{feature} between {round(float(min_max.get('min')), 2)} and {round(float(min_max.get('max')), 2)}"

        rule_json['rule'] = rule
        if count < len(features) - 1:
            rule_json['left'] = add_conditions(features[count + 1], count + 1)
        else:
            rule_json['left'] = tree
        return rule_json

    try:
        print("inside generate_boundries")
        return add_conditions(features[0], 0)

    except Exception as e:
        print(f"error occoured in generating boundries {e}")


def add_clusters_rules(features, features_val, tree):
    def add_conditions(feature, count):
        rule_json = {}
        rule_json['id'] = -1

        rule = f"{features[count]} == {features_val[count]}"
        rule_json['rule'] = rule
        if count < len(features) - 1:
            rule_json['left'] = add_conditions(features[count + 1], count + 1)
        else:
            rule_json['left'] = tree
        return rule_json

    try:
        print("inside generate_boundries")
        return add_conditions(features[0], 0)

    except Exception as e:
        print(f"error occoured in generating boundries {e}")


def make_request(body):
    rule_main = None
    try:
        url = cfg.Config.rule_converter_url
        x = requests.post(url, json=body)
        response_json = x.json()
        if response_json.get('respCode') is None or response_json.get('respCode') != 'SUCCESS':
            raise ValueError("rule engine gave error respose " + x.text)

        return json.dumps(response_json.get('guiRequest'))


    except Exception as e:
        print("error occoured in http requerst", e)
        raise RuntimeError(e)


class RuleGenerator(object):
    def __init__(self, df, dag_run_id, cluster_name, cluster_number, purchase_filtered):
        self.df = df
        self.dag_run_id = dag_run_id
        self.cluster_name = cluster_name
        self.cluster_name = cluster_number
        self.purchase = purchase_filtered
        pass

    def generate_rules(self, segementss, usage):
        segment_list = []
        for segements in segementss:
            if segements.recommended_product_id is None:
                continue
            usage_filter1 = usage[usage['msisdn'].isin(self.df['msisdn'])]
            # usage_filter1 = usage_filter1.compute()
            conci = int(segements.recommended_product_id)
            anti = [int(x) for x in segements.current_product.split("|")]
            anti.append(conci)
            df = form_data(p2=self.purchase, df=usage_filter1, anti_conci=anti)
            value_counts = df['label'].value_counts()
            if 0 in value_counts.index and 1 in value_counts.index:
                if value_counts.loc[0] >= 100 and value_counts.loc[1] >= 100:
                    print("Both values have at least two entries")
                else:
                    print("At least one value doesn't have two entries")
                    continue
            else:
                print("Both values are not present in the column")
                continue

            clf1, features = rls.train_model(df)
            decision_tree_obj = rls.DecisionTreeConverter(clf1, features, ['differentpack', 'whatsappPack'],
                                                          df[df['label'] == 1])
            treetojson = decision_tree_obj.get_json()
            print("tree fromed", treetojson)
            prune_tree = rls.pruneTree(root=json.loads(treetojson), columns_for_dummies=[])
            df_temp = df[df['label'] == 1]
            df1 = df_temp.agg(['min', 'max'])
            df_json = json.loads(df1.to_json())
            prune_tree = generate_boundries(features, df_json, prune_tree)
            segement_names = cfg.Config.segement_names
            segement_values = segements.segment_name.split("-")[:-1]
            prune_tree1 = add_clusters_rules(segement_names, segement_values, prune_tree)
            segements.rule = json.dumps(prune_tree1)
            # segements.rule = make_request(prune_tree1)
            segment_list.append(segements)
        return segment_list


def rule_generation(dag_run_id, db):
    try:

        file_name_dict = cfg.get_file_names()
        print("finding trend ongoing ")
        data = {}
        for month in cfg.Config.usage_no_months:
            data[month] = dd.read_csv(os.path.join(cfg.Config.etl_location, file_name_dict.get("usage").get(month)),
                                      dtype=f.Features.CUSTOMER_DTYPES)
        months = cfg.Config.usage_no_months
        temp_df = None

        for month in months:
            df = data.get(month)
            df = df.fillna(0)
            total_revenue = f.Features.CUSTOMER_TOTAL_REVENUE[0]
            df['tot_rev'] = df[total_revenue]
            df = otliner_removal(df.copy())
            df = df.add_prefix(f"{month}_")
            df = df.rename(columns={f"{month}_msisdn": "msisdn"})
            if temp_df is None:
                temp_df = df
            else:

                temp_df = temp_df.merge(df, on='msisdn', how="left")
                temp_df = temp_df.fillna(0)
        temp_df = temp_df.compute()

        result_dict_path = os.path.join(cfg.Config.ml_location, dag_run_id, "purchased_for_association", 'dict.pickle')
        data_path = os.path.join(cfg.Config.ml_location, dag_run_id, "dict.pickle")
        data_dict = load_picke_file(data_path)
        pack_info = os.path.join(cfg.Config.etl_location, 'pack_info.csv')
        pack_info_df = pd.read_csv(pack_info)
        data = load_picke_file(result_dict_path)

        filtered_dict = {k: v for k, v in data.items()}
        filtered_data__dict = {k: v for k, v in data_dict.items()}

        for item, val in filtered_dict.items():
            data = pd.read_csv(filtered_data__dict.get(item))
            purchase = pd.read_csv(val)
            for cluster in data['label'].unique():
                segements_name = f"{item}-{str(cluster)}"
                data_temp = data[data['label'] == cluster]
                purchase_filtered = purchase[purchase[msisdn_name].isin(data_temp[msisdn_name])]

                print('len of purchase in association_process ', len(purchase_filtered))
                rg = RuleGenerator(df=data, dag_run_id=dag_run_id, cluster_name=item, cluster_number=cluster,
                                   purchase_filtered=purchase_filtered)
                segements = SegementRepo.findByAutoPilotIdAndSegementNameAll(db=db, _id=dag_run_id,
                                                                             segement_name=segements_name,
                                                                             cluster_number=int(cluster))
                if segements is None:
                    continue
                segements_new = rg.generate_rules(segements, temp_df)
                for seg in segements_new:
                    if seg is not None:
                        SegementRepo.update(db=db, item_data=seg)


    except Exception as e:
        print(e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail="error occoured rule_generation" + str(e))
    return schemas.BaseResponse(statusCode=200, message="success", status="success")

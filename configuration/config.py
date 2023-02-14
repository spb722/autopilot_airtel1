# -*- coding: utf-8 -*-
# @Time    : 09-06-2022 15:18
# @Author  : sachin
# @Email   : spb722@gmail.com
# @File    : config.py
# @Software: PyCharm


#
# def get_file_names():
#     return {
#         "usage": {
#             "m1": "m1_usage.csv",
#             "m2": "m2_usage.csv",
#             "m3": "m3_usage.csv"
#         },
#         "recharge": {
#             "m1": "m1_recharge.csv",
#             "m2": "m2_recharge.csv",
#             "m3": "m3_recharge.csv"
#
#         },
#         "purchase": {
#             "m1": "m1_purchase.csv",
#             "m2": "m2_purchase.csv",
#             "m3": "m3_purchase.csv"
#
#         }
#
#     }
#
# class Config:
#     # source_purchase_and_etl_location = "/log/magikuser/tnm_autopilot_calender_month"
#     # purchase_location = "/log/magikuser/autopilot_data/purchase"
#     # etl_location = "/log/magikuser/autopilot_data/etl/etl_with_130_columns"
#     # pack_info_location = '/log/magikuser/autopilot_data/packinfo'
#     # ml_location = '/log/magikuser/autopilot_data/ml'
#     #
#     source_purchase_and_etl_location = "/data/autopilot/etl_files"
#     purchase_location = "/data/autopilot/etl_files"
#     recharge_location = "/data/autopilot/etl_files"
#     etl_location = "/data/autopilot/etl_files"
#     pack_info_location = '/data/autopilot/ml'
#     ml_location = '/data/autopilot/ml'
#
#     usage_dtypes = {'total_data_revenue': 'float32', 'og_total_voice_revenue': 'float32'}
#
#     usage_no_months = ['m1', 'm2', 'm3']
#     recharge_no_months = ['m1', 'm2', 'm3']
#     purchase_no_months = ["m2", "m3"]
#     purchase_no_months_seg = ['m1',"m2", "m3"]
#     threshold = 0.50
#
#
#     def to_json(self):
#         return {
#             'purchase_location': self.purchase_location,
#             'usage_location': self.etl_location,
#             'pack_info_location': self.pack_info_location,
#             'ml_location': self.ml_location,
#             'recharge_location':self.recharge_location
#         }
#
#
#
#
#     def get_banding_confitions(self):
#         return {
#             "common": {"Zero": 1, "<=N50": 2, "N50 - N100": 3, "N100 - N200": 4, "N200 - N300": 5, "N300 - N400": 6,
#                        "N400 - N500": 7,
#                        "N500 - N1000": 8, "N1000 - N1200": 9, "N1200 - N1500": 10, "N1500 - N2000": 11,
#                        "N2000 - N3000": 12,
#                        "N3000 - N4000": 13, "N4000 - N5000": 14, "N5000-N10000": 15, "N10000 - N20000": 16,
#                        "N20000 - N30000": 17, "N30000+": 18}
#         }
#
# feature_mapping = {
#               'Data':['Data (MB)','Price'],
#               'Voice':['Voice','Price']}
#
#
#
# source_purchase_and_etl_location = "/log/magikuser/tnm_autopilot_calender_month"
# purchase_location = "/log/magikuser/autopilot_data/purchase"
# etl_location = "/log/magikuser/autopilot_data/etl/etl_with_130_columns"
# pack_info_location = '/log/magikuser/autopilot_data/packinfo'
# ml_location = '/log/magikuser/autopilot_data/ml'
#
# products_to_filter = ["Yanga_", "Yanga_"]
# base_path = ''
# product_id_field_name = "product_id"
# pack_name = "bundle_name"
# group_type = "group_type"
# columns = []
#
# dtype = {}
#
# non_sub_kpi_columns = []
#
# non_count_kpis = []
#
# dont_needed_pid1 = []
#
# dont_needed_pid = []
#
# dont_needed_pname = []


def get_file_names():
    return {
        "usage": {
            "m1": "usage_m1_20230206160823.csv",
            "m2": "usage_m2_20230206160823.csv",
            "m3": "usage_m3_20230206160823.csv"
        },
        "recharge": {
            "m1": "recharge_m1_20230206165903.csv",
            "m2": "recharge_m2_20230206165903.csv",
            "m3": "recharge_m3_20230206165903.csv"

        },
        "purchase": {
            "m1": "purchase_m1_20230206203137.csv",
            "m2": "purchase_m2_20230206203137.csv",
            "m3": "purchase_m3_20230206203137.csv"

        }

    }


class Config:
    # source_purchase_and_etl_location = "/log/magikuser/tnm_autopilot_calender_month"
    # purchase_location = "/log/magikuser/autopilot_data/purchase"
    # etl_location = "/log/magikuser/autopilot_data/etl/etl_with_130_columns"
    # pack_info_location = '/log/magikuser/autopilot_data/packinfo'
    # ml_location = '/log/magikuser/autopilot_data/ml'
    #
    source_purchase_and_etl_location = "C:/Users/spb72/OneDrive/Documents/6d/etl_files"
    purchase_location = "C:/Users/spb72/OneDrive/Documents/6d/etl_files"
    recharge_location = "C:/Users/spb72/OneDrive/Documents/6d/etl_files"
    etl_location = "C:/Users/spb72/OneDrive/Documents/6d/etl_files"
    pack_info_location = 'C:/Users/spb72/OneDrive/Documents/6d/etl_files'
    ml_location = 'C:/Users/spb72/OneDrive/Documents/6d'

    usage_dtypes = {'total_data_revenue': 'float32', 'og_total_voice_revenue': 'float32'}

    usage_no_months = ['m1', 'm2', 'm3']
    recharge_no_months = ['m1', 'm2', 'm3']
    purchase_no_months = ["m2", "m3"]
    purchase_no_months_seg = ['m1', "m2", "m3"]
    threshold = 0.50

    def to_json(self):
        return {
            'purchase_location': self.purchase_location,
            'usage_location': self.etl_location,
            'pack_info_location': self.pack_info_location,
            'ml_location': self.ml_location,
            'recharge_location': self.recharge_location
        }

    def get_banding_confitions(self):
        return {
            "common": {"Zero": 1, "very_low": 2, "low": 3, "medium": 4, "high": 5, "high_high": 6},
            "common_reverse": {1: "Zero", 2: "very_low", 3: "low", 4: "medium", 5: "high",
                               6: "high_high"}
        }

    feature_unit_list = ['unit_in_mb', 'price']


# feature_mapping = {
#     'Data': ['Data (MB)', 'Price'],
#     'Voice': ['Voice', 'Price']}

source_purchase_and_etl_location = "/log/magikuser/tnm_autopilot_calender_month"
purchase_location = "/log/magikuser/autopilot_data/purchase"
etl_location = "/log/magikuser/autopilot_data/etl/etl_with_130_columns"
pack_info_location = '/log/magikuser/autopilot_data/packinfo'
ml_location = '/log/magikuser/autopilot_data/ml'

products_to_filter = ["Yanga_", "Yanga_"]
base_path = ''
product_id_field_name = "product_id"
pack_name = "product_name"
group_type = "group_type"
columns = []

dtype = {}

non_sub_kpi_columns = []

non_count_kpis = []

dont_needed_pid1 = []

dont_needed_pid = []

dont_needed_pname = []

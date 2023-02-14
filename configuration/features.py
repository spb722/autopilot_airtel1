# @Author:      sachin pb
# @Time:        22-12-2022 15:54
# @Email:       spb722@gmail.com
#
# class Features:
#     MSISDN_COL_NAME = 'msisdn'
#     TRANSACTION_CATEG_FEATURES = ['trans_dt']
#     TRANSACTION_CONTI_FEATURES = ['msisdn', 'refill_id', 'total_refill_cnt']
#     TRANSACTION_FEATURES = TRANSACTION_CATEG_FEATURES + TRANSACTION_CONTI_FEATURES
#     TRANSACTION_DTYPES = {'msisdn': 'float64',
#                           'total_trans_amount': 'object'}
#     TRANSACTION_PURCHASE_DATE_NAME = 'trans_dt'
#     TRANSACTION_PRICE_COL_NAME = 'total_refill_price'
#     TRANSACTION_COUNT_COL_NAME = 'total_refill_cnt'
#     TRANSACTION_NEEDED_COL = ['msisdn', 'total_refill_cnt']
#     TRANSACTION_PRODUCT_NAME = 'refill_id'
#
#     PACK_CONTI_FEATURES = []
#     PACK_CATEG_FEATURES = ['Product_Type', 'Validity', 'Voice', 'Data (MB)', 'Price', 'Product_Code']
#     PACK_INFO_DTYPES = {'Validity': 'int'}
#     ALL_PACK_FEATURES = PACK_CONTI_FEATURES + PACK_CATEG_FEATURES
#     PACK_INFO_CATEGORY = 'Product_Type'
#     PACK_INFO_PACK_COLUMN_NAME = 'Product_Code'
#     PACK_INFO_PACK_PRICE_COLUMN_NAME = 'Price'
#     CUSTOMER_CATEG_FEATURES = []
#     CUSTOMER_CONTI_FEATURES = []
#     CUSTOMER_CATEG_NEEDED_COL = ['msisdn', 'og_total_voice_mou', 'total_data_usage']
#     # 0 index for inbundle 1 index for outbundled  2 index for total
#     CUSTOMER_VOICE_COL_NAME = ['og_total_voice_mou']
#     CUSTOMER_VOICE_REVENUE = ["", "", "og_total_voice_revenue"]
#     CUSTOMER_DATA_COL_NAME = ['total_data_usage']
#     CUSTOMER_DATA_REVENUE = ["", "", "total_data_revenue"]
#
#     CUSTOMER_FEATURES = CUSTOMER_CATEG_FEATURES + CUSTOMER_CONTI_FEATURES
#
#     CUSTOMER_DTYPES = {'total_data_revenue': 'float32', 'og_total_voice_revenue': 'float32'}
#
#     RECHARGE_DTYPES = {'recharge_refill_cnt': 'float32', 'total_recharge_trans_amount': 'float32'}
#     RECHARGE_COUNT_COL_NAME = 'recharge_refill_cnt'
#     RECHARGE_NEEDED_COL = ['msisdn', 'recharge_refill_cnt']
#
#     LABEL1 = 'downtrend'
#     LABEL2 = 'uptrend'
#     LABEL3 = 'zigzag'
#     LABEL4 = 'flat'
#
#     def to_json(self):
#         return {
#             'purchase_features': self.TRANSACTION_FEATURES,
#             'usage_features': self.CUSTOMER_FEATURES,
#             'pack_info_features': self.ALL_PACK_FEATURES,
#             'pack_info_dtypes': self.PACK_INFO_DTYPES,
#             'usage_dtypes': self.CUSTOMER_DTYPES
#
#         }


class Features:
    PACK_INFO_VALIDITY_NAME = "validity_in_days"
    MSISDN_COL_NAME = 'msisdn'
    TRANSACTION_CATEG_FEATURES = ['event_date']
    TRANSACTION_CONTI_FEATURES = ['msisdn', 'product_id', 'total_package_id']
    TRANSACTION_FEATURES = TRANSACTION_CATEG_FEATURES + TRANSACTION_CONTI_FEATURES
    TRANSACTION_DTYPES = {'msisdn': 'float64',
                          'total_package_revenue': 'float32'}
    TRANSACTION_PURCHASE_DATE_NAME = 'event_date'
    TRANSACTION_PRICE_COL_NAME = 'total_package_revenue'
    TRANSACTION_COUNT_COL_NAME = 'total_package_id'
    TRANSACTION_NEEDED_COL = ['msisdn', 'total_package_id']
    TRANSACTION_PRODUCT_NAME = 'product_id'

    PACK_CONTI_FEATURES = []
    PACK_CATEG_FEATURES = ['product_type', 'validity_in_days', 'unit_in_mb', 'price', 'product_id']
    PACK_INFO_DTYPES = {'validity_in_days': 'int', 'unit_in_mb': 'float32'}
    ALL_PACK_FEATURES = PACK_CONTI_FEATURES + PACK_CATEG_FEATURES
    PACK_INFO_CATEGORY = 'product_type'
    PACK_INFO_PACK_COLUMN_NAME = 'product_id'
    PACK_INFO_PACK_PRICE_COLUMN_NAME = 'price'
    CUSTOMER_CATEG_FEATURES = []
    CUSTOMER_CONTI_FEATURES = []
    CUSTOMER_CATEG_NEEDED_COL = ['msisdn', 'total_voice_usage', 'total_data_usage']
    # 0 index for inbundle 1 index for outbundled  2 index for total
    CUSTOMER_VOICE_COL_NAME = ['total_voice_usage']
    CUSTOMER_VOICE_REVENUE = ["", "", "og_total_voice_revenue"]
    CUSTOMER_DATA_COL_NAME = ['total_data_usage']
    CUSTOMER_TOTAL_REVENUE = ['total_revenue']
    CUSTOMER_DATA_REVENUE = ["", "", "total_data_rev"]

    CUSTOMER_FEATURES = CUSTOMER_CATEG_FEATURES + CUSTOMER_CONTI_FEATURES

    CUSTOMER_DTYPES = {'total_revenue': 'float32'}

    RECHARGE_DTYPES = {'total_recharge_cnt': 'float32', 'total_rechargeamount': 'float32'}
    RECHARGE_COUNT_COL_NAME = 'total_recharge_cnt'
    RECHARGE_NEEDED_COL = ['msisdn', 'total_recharge_cnt']

    LABEL1 = 'downtrend'
    LABEL2 = 'uptrend'
    LABEL3 = 'zigzag'
    LABEL4 = 'flat'

    def to_json(self):
        return {
            'purchase_features': self.TRANSACTION_FEATURES,
            'usage_features': self.CUSTOMER_FEATURES,
            'pack_info_features': self.ALL_PACK_FEATURES,
            'pack_info_dtypes': self.PACK_INFO_DTYPES,
            'usage_dtypes': self.CUSTOMER_DTYPES

        }

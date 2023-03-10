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
    PACK_INFO_SUB_CATEGORY = 'bundle_type'
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
    CUSTOMER_NEEDED_COLUMN = [
                              'onnet_revenue',
                              'onnet_duration',
                              'onnet_usage_ma',
                              'onnet_usage_da_cur_account',
                              'onnet_usage_da',
                              'onnet_usage',
                              'onnet_voice_count',
                              'onnet_da_revenue',
                              'offnet_revenue',
                              'offnet_duration',
                              'offnet_usage_ma',
                              'offnet_usage_da_cur_account',
                              'offnet_usage_da',
                              'offnet_usage',
                              'offnet_voice_count',
                              'offnet_da_revenue',
                              'idd_revenue',
                              'idd_duration',
                              'idd_usage_ma',
                              'idd_usage_da_cur_account',
                              'idd_usage_da',
                              'idd_usage',
                              'idd_voice_count',
                              'idd_da_revenue',
                              'voice_rmg_revenue',
                              'voice_rmg_duration',
                              'voice_rmg_usage_ma',
                              'voice_rmg_usage_da_cur_account',
                              'voice_rmg_usage_da',
                              'voice_rmg_usage',
                              'voice_rmg_count',
                              'voice_rmg_da_revenue',
                              'data_rmg_revenue',
                              'data_rmg_duration',
                              'data_rmg_usage_ma',
                              'data_rmg_usage_da_cur_account',
                              'data_rmg_usage_da',
                              'data_rmg_usage',
                              'data_rmg_da_revenue',
                              'data_revenue',
                              'data_duration',
                              'data_usage_ma',
                              'data_usage_da_cur_account',
                              'data_usage_da',
                              'data_usage',
                              'da_data_rev',
                              'sms_revenue',
                              'sms_usage_ma',
                              'sms_usage_da_cur_account',
                              'sms_usage_da',
                              'sms_usage',
                              'sms_da_revenue',
                              'sms_idd_revenue',
                              'sms_idd_duration',
                              'sms_idd_usage_ma',
                              'sms_idd_usage_da_cur_account',
                              'sms_idd_usage_da',
                              'sms_idd_usage',
                              'sms_idd_da_revenue',

                              'onn_rev',
                              'off_rev',
                              'total_data_rev',
                              'vas_rev',
                              'vas_rev_others',
                              'total_revenue',
                              'total_voice_count',
                              'total_voice_duration',
                              'total_mainaccount_data_usage',
                              'total_sms_count',
                              'total_package_count',
                              'recharge_count',
                              'total_other_vas_count',
                              'voice_rev',
                              'sms_rev',
                              'total_voice_usage',
                              'total_data_usage',
                              'total_sms_usage']

    CUSTOMER_FEATURES = CUSTOMER_CATEG_FEATURES + CUSTOMER_CONTI_FEATURES

    CUSTOMER_DTYPES = {'total_revenue': 'float32'}

    # RECHARGE_DTYPES = {'total_recharge_cnt': 'float32', 'total_rechargeamount': 'float32'}
    # RECHARGE_COUNT_COL_NAME = 'total_recharge_cnt'
    # RECHARGE_NEEDED_COL = ['msisdn', 'total_recharge_cnt']

    RECHARGE_DTYPES = {'total_package_id': 'float32', 'total_package_revenue': 'float32'}
    RECHARGE_COUNT_COL_NAME = 'total_package_id'
    RECHARGE_NEEDED_COL = ['msisdn', 'total_package_id']

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

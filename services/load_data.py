# @Filename:    load_data.py
# @Author:      sachin pb
# @Time:        22-12-2022 16:41
# @Email:       spb722@gmail.com

from icecream import ic

import dask.dataframe as dd
import os
import configuration.config as cfg
import configuration.features as f
import traceback

config = cfg.Config().to_json()
features = f.Features().to_json()


class TNMData:
    def __init__(self,
                 purchase_df,
                 usage_df,
                 pack_df):
        self.purchase_df = purchase_df
        self.usage_df = usage_df
        self.pack_df = pack_df
def load_data(data_info=None,dag_run_id=None):
    dataframe_dict = {}

    for datas in ['purchase_', 'usage_', 'pack_info_']:
        ic(f" reading {datas}  files")
        df = None
        try:
            file_names = data_info.get(datas + "file_names")
            if isinstance(file_names, list):
                ic(f"got a list of {datas} files", file_names)
                for file_name in file_names:
                    ic(f"going to read {file_name}")
                    print('file_name1  is ', file_name)
                    if df is None:
                        print('file_name2  is ', file_name)
                        df = dd.read_csv(os.path.join(config.get(f"{datas}location"),  file_name),
                                         usecols=features.get(f"{datas}features"))
                    else:
                        print('file_name3  is ', file_name)

                        print('config.get(f"{datas}location") ', datas,'   is ', config.get(f"{datas}location"))
                        print('features.get(f"{datas}features") ', datas,' is ', features.get(f"{datas}features"))

                        print('(os.path.join(config.get(f"{datas}location"), file_name) ', datas,' is ',
                              (os.path.join(config.get(f"{datas}location"),  file_name)))

                        df_temp = dd.read_csv(os.path.join(config.get(f"{datas}location"),  file_name),
                                              usecols=features.get(f"{datas}features"))

                        print('len(df_temp)', len(df_temp))
                        df = dd.concat([df, df_temp])

                        print('len(df)', len(df))
            else:
                print('file_names  is ', file_names)

                print('config.get(f"{datas}location") ', datas,' is ', config.get(f"{datas}location"))
                print('features.get(f"{datas}features") ', datas,' is ', features.get(f"{datas}features"))

                print('(os.path.join(config.get(f"{datas}location"), file_name) ', datas,' is ',
                      (os.path.join(config.get(f"{datas}location"),  file_names)))

                print('features.get(f"{datas}dtypes") ', datas,' is ', features.get(f"{datas}dtypes"))

                df = dd.read_csv(os.path.join(config.get(f"{datas}location"),  file_names),
                                 usecols=features.get(f"{datas}features"), dtype=features.get(f"{datas}dtypes"))

                print(f"len({datas} ) of ", len(df))
        except Exception as e:
            print(e)
            ic(f"no {datas} read")

        dataframe_dict[f"{datas}data"] = df

        # traceback.print_exc()
    tnmdata = TNMData(dataframe_dict.get('purchase_data'),
                      dataframe_dict.get('usage_data'),
                      dataframe_dict.get('pack_info_data'))
    ic("tnm class loaded")
    return tnmdata





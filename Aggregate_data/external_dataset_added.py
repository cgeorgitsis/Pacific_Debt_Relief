import pandas as pd
import reusable_functions as rf
from Aggregate_data.config.config_utils import ConfigUtils
from Aggregate_data.config.__init__ import Config
from logger import Logger


def read_csv_file() -> pd.DataFrame:
    return rf.read_csv_file(ConfigUtils.conf.path_to_us_census_bureau_3rd_party_data)


def merge_datasets(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df2.rename({'Zipcode': 'Zip_1'}, axis=1, inplace=True)
    df2['Zip_1'] = df2['Zip_1'].astype(str)
    return pd.merge(df1, df2, on='Zip_1', how='left')


def format_external_dataset(conf: Config, logger: Logger):
    add_description_df = rf.read_df_from_pickle_format(conf, 'add_description_df')
    external_df = read_csv_file()
    add_us_census_bureau_df = merge_datasets(add_description_df, external_df)
    rf.store_df_in_pickle_format(ConfigUtils.conf, add_us_census_bureau_df=add_us_census_bureau_df)

import pandas as pd
import reusable_functions as rf
from Aggregate_data.config.config_utils import ConfigUtils
from Aggregate_data.config.__init__ import Config
from logger import Logger


def unzip_zipcode(pdr_file: pd.DataFrame) -> pd.DataFrame:
    pdr_file['Zip'] = pdr_file['Zip'].astype(str).str.zfill(9)
    pdr_file = pdr_file.assign(Zip_1=pdr_file['Zip'].astype(str).str[:5], Zip_2=pdr_file['Zip'].astype(str).str[5:])
    pdr_file = pdr_file.drop(['Zip'], axis=1)
    pdr_file['Zip_2'] = pdr_file['Zip_2'].str.replace('-', '')
    return pdr_file


def add_description(given_df: pd.DataFrame) -> pd.DataFrame:
    new_df = rf.read_csv_file(ConfigUtils.conf.path_status_description)
    new_df = new_df.drop_duplicates(subset='Status', keep="last")
    df = pd.merge(given_df, new_df, on='Status', how='left')
    df = df.loc[:, ['UUID', 'gender', 'Status', 'Description', 'Customer_contacted_status',
                    'Customer_intention', 'Mail_number', 'Calls_number', 'Zip_1', 'Zip_2', 'Address', 'City', 'State',
                    'Debt Amount', 'Date Added', 'Lead Source', 'Direct Mail DID', 'Phone', 'Date', 'Queue', 'Trunk',
                    'Call Time', 'Exit Reason', 'First Name', 'Last Name', 'Lead purchased', 'New_lead']]
    return df


def format_description_dataset(conf: Config, logger: Logger):
    add_phone_and_trunk_df = rf.read_df_from_pickle_format(conf, 'add_phone_and_trunk_df')
    new_df = unzip_zipcode(add_phone_and_trunk_df)
    add_description_df = add_description(new_df)
    rf.store_df_in_pickle_format(ConfigUtils.conf, add_description_df=add_description_df)

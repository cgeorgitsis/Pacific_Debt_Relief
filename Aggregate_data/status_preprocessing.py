import pandas as pd
import os, pickle
import reusable_functions as rf
import phone_and_trunk_preprocessing as ptp
from Aggregate_data.config.config_utils import ConfigUtils
from Aggregate_data.config.__init__ import Config
from logger import Logger


def create_customer_intention_column(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy.loc[:, 'Customer_intention'] = df_copy['Status'].apply(map_customer_intention)
    return df_copy


def map_customer_intention(status: str) -> str:
    if status in ['Client', 'C1 Client', 'Hot', 'Scheduled Appointment', 'Credit Counseling Lead']:
        return 'Positive'
    else:
        return 'Negative'


def clear_status_with_phone_dataset() -> pd.DataFrame:
    phone_status_df = rf.read_csv_file(os.getenv('PATH_PHONE_20230308'))
    cleared_ref_id_df = clear_status_dataset(phone_status_df)
    contained_phone_column_df = ptp.create_phone_column(cleared_ref_id_df)
    return contained_phone_column_df


def clear_status_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset='DM Reference ID')
    df_copy = df.copy()
    df_copy.dropna(subset=['DM Reference ID'], inplace=True)
    df_copy = df_copy.drop(['Id'], axis=1)
    df_copy['Date Added'] = pd.to_datetime(df_copy['Date Added'])
    # drop values from column --> status that have the value --> Test Lead
    df_copy = df_copy[df_copy.Status != 'Test Lead']
    df_copy = df_copy.reset_index(drop=True)
    # Convert www.pdoffer.com/Sharon94954 to Sharon94954
    df_copy.loc[:, 'DM Reference ID'] = df_copy['DM Reference ID'].str.replace(r'.*/([^/]+)$', r'\1', regex=True)
    df_copy['DM Reference ID'] = df_copy['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    return rf.check_number_of_digits(df_copy)


def merge_2_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    # update the values of left dataframe based on values of the right dataframe where the merge keys match
    new_df = pd.merge(df1, df2, on='UUID', how='left')
    new_df.drop(['Lead Source_y'], axis=1, inplace=True)
    new_df.rename({'Lead Source_x': 'Lead Source'}, axis=1, inplace=True)
    return new_df


def preprocess_merge_df(df: pd.DataFrame) -> pd.DataFrame:
    # Step_1: keep only digits from merged_df['DM Reference ID']
    df['DM Reference ID'] = df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    return df


def union_datasets_containing_status_feature_with_different_formats(df1: pd.DataFrame, df2: pd.DataFrame)\
        -> pd.DataFrame:
    """
    This code will first concatenate the 2 dataframes with different format. Next we sort the DataFrame by the
    'Date Added' column in descending order, and then drop duplicates based on the 'DM Reference ID' column while
    keeping the first occurrence of each unique value. The resulting DataFrame will contain only the rows with the most
    recent 'Date Added' value for each unique 'DM Reference ID' value.
    :param df1: A dataset that does not contain the feature 'Phone'
    :param df2: A dataset that contains the feature 'Phone'
    :return: The concatenated dataset
    """
    df_concatenated = pd.concat([df1, df2], axis=0,
                                ignore_index=True, sort=False)
    df_concatenated = df_concatenated.sort_values('Date Added', ascending=False).drop_duplicates(
        subset='DM Reference ID', keep='first')
    return df_concatenated


def find_clients_with_status_that_does_not_exist_in_pdr_files(df_reference: pd.DataFrame, df_status: pd.DataFrame):
    """
    This function finds all the clients that we have a status from them but are not contained in current PDR Files that
    Pacific debt has sent to us.
    :param df_reference: Matches the DM Reference ID with the UUID
    :param df_status: Contains the status of each client
    :return:
    """
    missing_dm_reference_ids = df_status[
        ~df_status['DM Reference ID'].isin(df_reference['DM Reference ID'])]
    missing_dm_reference_ids.to_csv(os.environ.get('PATH_MISSING_CLIENTS'), sep=',', encoding='utf-8',
                                    index=False)


def format_status_dataset(conf: Config, logger: Logger):
    prospects_pdr_files = rf.read_df_from_pickle_format(conf, 'prospects_pdr_files')
    lead_reference_lookup = rf.read_df_from_pickle_format(conf, 'lead_reference_lookup')

    sabino_ingestion = rf.read_csv_file(ConfigUtils.conf.path_status)
    status_dataset_without_phone_format = clear_status_dataset(sabino_ingestion)
    status_dataset_with_phone_format = clear_status_with_phone_dataset()
    union_status_df = union_datasets_containing_status_feature_with_different_formats \
        (status_dataset_without_phone_format, status_dataset_with_phone_format)
    lead_reference_lookup['DM Reference ID'] = lead_reference_lookup['DM Reference ID'].astype(str)
    merged_df = pd.merge(lead_reference_lookup, union_status_df, on='DM Reference ID', how='right')
    merged_df = merged_df.dropna(subset=['UUID'])
    find_clients_with_status_that_does_not_exist_in_pdr_files(lead_reference_lookup, union_status_df)
    second_merged_df = merge_2_dataframes(prospects_pdr_files, merged_df)
    final_status_df = preprocess_merge_df(second_merged_df)
    final_status_df.rename({'DM_Reference_ID': 'DM Reference ID'}, axis=1, inplace=True)
    final_df = create_customer_intention_column(final_status_df)
    final_df_copy = final_df.copy()
    final_df_copy.drop(['DM Reference ID'], axis=1, inplace=True)
    rf.store_df_in_pickle_format(ConfigUtils.conf, add_status_df=final_df_copy)


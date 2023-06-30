import pandas as pd
import numpy as np
import reusable_functions as rf
from Aggregate_data.config.__init__ import Config
from Aggregate_data.config.config_utils import ConfigUtils
from logger import Logger


def consolidate_status_in_a_single_feature(df: pd.DataFrame) -> pd.DataFrame:
    # create new 'Status' column using apply method
    df['Status'] = df.apply(
        lambda row: row['CRM Status'] if row['Status'] == 'Aged - Uncontacted' and pd.notna(row['CRM Status']) and row[
            'CRM Status'] != '' else row['Status'], axis=1)

    # drop the 'CRM Status' column because it's no longer needed
    df.drop(columns=['CRM Status'], inplace=True)
    return df


def create_customer_contacted_status_column(df: pd.DataFrame) -> pd.DataFrame:
    df['Customer_contacted_status'] = df.apply(map_customer_contacted_status, axis=1)
    return df


def map_customer_contacted_status(row: pd.DataFrame) -> str:
    """
    Our new column takes the value 'contacted' when:

    the customer's status is 'Contacted'
    the customer's status is not one of the following:
    * Aged - Uncontacted
    * Hot
    * Nurture
    * Bogus Lead
    * Disconnected Number
    * New Lead
    * Short Call
    """
    uncontacted_statuses = ['Aged - Uncontacted', 'Hot', 'Nurture', 'Disconnected Number', 'New Lead',
                            'Duplicate Lead', 'DO NOT CALL', 'DM Opt-Out', 'Short Call', 'Test Lead']
    if (row['Status'] not in uncontacted_statuses) or (not pd.isna(row['Trunk'])):
        return 'contacted'
    else:
        return 'uncontacted'


def handle_empty_values_in_status(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df['Status'].isnull() | (df['Status'] == ''), 'Status'] = 'Aged - Uncontacted'
    return df


def merge_df_with_report_and_preprocessing(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df1['Phone'] = df1['Phone'].apply(lambda x: rf.keep_number(x))
    df1['Phone'] = df1['Phone'].astype(str)
    # Group by phone and keep the latest date of a call
    report_df = df2.groupby('Phone').apply(lambda x: x[x['Date'] == x['Date'].max()]).reset_index(drop=True)
    report_df.Phone = report_df.Phone.astype(str)
    # Needs to drop duplicates, else it does not work right
    report_df = report_df.drop_duplicates(subset='Phone', keep="last")
    merged = pd.merge(df1, report_df, on='Phone', how='left')
    return merged


def check_for_common_values_in_report_and_phone_datasets(new_df: pd.DataFrame, call_center_df: pd.DataFrame):
    call_center_df.loc[:, 'Caller ID'] = call_center_df['Caller ID'].astype(str)
    new_df['Phone'] = new_df['Phone'].astype(str)
    new_df['Phone'] = new_df['Phone'].apply(lambda x: rf.keep_number(x))
    common_values = new_df[new_df['Phone'].isin(call_center_df['Caller ID'])]
    print('There exist', len(common_values), 'common values in report and phone dataset')


def create_column_number_of_calls(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only the rows where Caller ID contains only digits. Drop values like ('Restricted','0Anonymous','00asterisk')
    df = df[df['Caller ID'].apply(lambda x: x.isnumeric())]
    # Stores all rows that are not digits
    problematic_rows = df[pd.to_numeric(df['Caller ID'], errors='coerce').isna()]
    # problematic_rows.to_csv('Caller_ID_problematic_rows.csv', sep=',', encoding='utf-8', index=False)
    df.loc[:, 'Calls_number'] = df.groupby('Caller ID', dropna=False)['Caller ID'].transform('count').astype(int)
    return df


def merge_dataframes(df1: pd.DataFrame, phone: pd.DataFrame) -> pd.DataFrame:
    # Needs to drop duplicates, else it does not work right
    phone = phone.drop_duplicates(subset='DM Reference ID', keep="last")
    # update the values of left dataframe based on values of the right dataframe where the merge keys match
    df = pd.merge(df1, phone, on='UUID', how='left')
    return df


def create_phone_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Description: This code creates a new column called "phone" and assigns it the value of "Mobile phone" if it is not
    NaN. If "Mobile phone" is NaN, it checks "Home phone", and if this is not NaN, it assigns it to the "phone" column.
    If both "Mobile phone" and "Home phone" are NaN, the value from "Work phone" is assigned to the "phone" column.
    :param df: A dataframe containing 4 columns (Customer id, Mobile phone, Home phone, Work phone)
    :return: A new dataframe that contains the new column==Phone
    """
    df['Phone'] = np.where(pd.notna(df['Mobile Phone']), df['Mobile Phone'],
                           np.where(pd.notna(df['Home Phone']), df['Home Phone'],
                                    df['Work Phone']))
    df.drop(['Mobile Phone', 'Home Phone', 'Work Phone'], axis=1, inplace=True)
    df['Phone'] = df['Phone'].astype(str)
    df.drop(df[df['Phone'] == 'nan'].index, inplace=True)
    df['Phone'] = df['Phone'].apply(lambda x: rf.keep_number(x))
    # Needs to clear DM reference ID column (ex: 39407-68469-A to 3940768469)
    df['DM Reference ID'] = df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    return df


def clear_phone_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset='DM Reference ID')
    df = df.dropna(subset=['DM Reference ID'])
    df.loc[:, 'Home Phone'] = df['Home Phone'].astype(str)
    df.loc[:, 'DM Reference ID'] = df['DM Reference ID'].str.replace(r'.*/([^/]+)$', r'\1', regex=True)
    return df


def concat_2_phone_columns_to_one(df: pd.DataFrame) -> pd.DataFrame:
    # concatenate the 'Phone_x' and 'Phone_y' columns into a new column named 'Phone'.
    # SOS Keep 'Phone_y' column else you will take Nan values
    df['Phone'] = df.apply(lambda row: row['Phone_y'] if row['Phone_x'] == row['Phone_y'] else row['Phone_y'], axis=1)
    # drop the 'Phone_x' and 'Phone_y' columns
    df = df.drop(['Phone_x', 'Phone_y'], axis=1)
    # remove duplicates from the 'Phone' column
    df = df.drop_duplicates(subset=['Phone'])
    # drop rows with empty strings in the 'Phone' column
    df = df[df['Phone'] != '']
    # drop Nan values
    df = df.dropna(subset=['Phone'])
    # convert the 'Phone' column of a pandas DataFrame 'df' from float to int64
    df['Phone'] = df['Phone'].astype('int64')
    return df


def format_phone_trunk_dataset(conf: Config, logger: Logger):
    add_status_df = rf.read_df_from_pickle_format(conf, 'add_status_df')
    lead_reference_lookup = rf.read_df_from_pickle_format(conf, 'lead_reference_lookup')

    phone_sabino = rf.read_csv_file(ConfigUtils.conf.path_phone)
    phone_dataset = clear_phone_dataset(phone_sabino)
    phone_df = create_phone_column(phone_dataset)
    lead_reference_lookup['DM Reference ID'] = lead_reference_lookup['DM Reference ID'].astype(str)
    initial_merged_df = pd.merge(lead_reference_lookup, phone_df, on='DM Reference ID', how='right')
    initial_merged_df = initial_merged_df.dropna(subset=['UUID'])
    merge_df = merge_dataframes(add_status_df, initial_merged_df)
    merge_df = merge_df.dropna(subset=['UUID'])
    merged_phone_columns = concat_2_phone_columns_to_one(merge_df)
    report_dataset = rf.read_csv_file(ConfigUtils.conf.path_final_call_center)
    new_report_dataset = create_column_number_of_calls(report_dataset.copy())
    check_for_common_values_in_report_and_phone_datasets(merged_phone_columns, new_report_dataset)
    new_report_dataset.rename(columns={'Caller ID': 'Phone'}, inplace=True)
    new_df = merge_df_with_report_and_preprocessing(merge_df, new_report_dataset)
    new_df_copy = new_df.copy()
    new_df_copy.drop(['DM Reference ID'], axis=1, inplace=True)
    new_df_copy = handle_empty_values_in_status(new_df_copy)
    df_contacted_status = create_customer_contacted_status_column(new_df_copy)
    final_df_with_one_status_feature = consolidate_status_in_a_single_feature(df_contacted_status)
    rf.store_df_in_pickle_format(ConfigUtils.conf, add_phone_and_trunk_df=final_df_with_one_status_feature)

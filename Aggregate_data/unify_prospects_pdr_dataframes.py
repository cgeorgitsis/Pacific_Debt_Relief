import pandas as pd
import numpy as np
import uuid
import reusable_functions as rf
from logger import Logger
from typing import Set
from Aggregate_data.config.__init__ import Config
from Aggregate_data.config.config_utils import ConfigUtils


def generate_unique_id(existing_ids: Set[int]) -> int:
    new_id = np.random.randint(1000000000, 9999999999, 1)[0]
    while new_id in existing_ids:
        new_id = np.random.randint(1000000000, 9999999999, 1)[0]
    return new_id


def give_prospects_a_unique_id_new(df: pd.DataFrame) -> pd.DataFrame:
    existing_ids = set(df['DM Reference ID'].values)
    empty_cells = df['DM Reference ID'] == ''
    for i, cell in enumerate(df.loc[empty_cells, 'DM Reference ID']):
        new_value = generate_unique_id(existing_ids)
        df.loc[empty_cells, 'DM Reference ID'].iloc[i] = new_value
        existing_ids.add(new_value)
    return df


def remove_non_digits(s) -> str:
    # Define function to remove non-digit characters from string
    return ''.join(filter(str.isdigit, str(s)))


def handle_inconsistent_zipcodes(df: pd.DataFrame) -> pd.DataFrame:
    """
    It has come to our attention that certain zip codes within our dataset are inconsistently formatted, with some
    appearing in the format '02585-2345' and others appearing in the format '025852345'. To address this issue, we have
    implemented a function that standardizes the formatting of all zip codes.
    :param df: The initial dataframe
    :return: The dataframe with the right form of Zipcode column
    """
    df['Zip'] = df['Zip'].apply(remove_non_digits)
    return df


def helper_concatenation_function(conf: Config, logger: Logger):
    prospects_df = rf.read_df_from_pickle_format(conf, 'prospects_df')
    pdr_merged_df = rf.read_df_from_pickle_format(conf, 'pdr_merged_df')

    df_list = [prospects_df, pdr_merged_df]
    new_df = pd.concat(df_list)
    fixed_zipcode = handle_inconsistent_zipcodes(new_df)

    # sort the dataframe by 'DM Reference ID' column in descending order
    # this will ensure that rows with non-null DM Reference ID values are sorted first
    # and will be kept when removing duplicates
    fixed_zipcode.sort_values(by='DM Reference ID', ascending=False, inplace=True)

    # replace empty strings in 'DM Reference ID' column with NaN
    # this will ensure that rows with missing DM Reference ID values are considered duplicates
    # and will be removed when removing duplicates
    fixed_zipcode['DM Reference ID'].replace('', np.nan, inplace=True)
    print('phase 1', fixed_zipcode.New_lead.value_counts())

    # create a new dataframe that contains only the rows with 'to_be_Scored' in the 'New_lead' column
    to_be_scored_df = fixed_zipcode[fixed_zipcode['New_lead'] == 'to_be_Scored']

    # create a new dataframe that contains all rows except those with 'to_be_Scored' in the 'New_lead' column
    other_df = fixed_zipcode[fixed_zipcode['New_lead'] != 'to_be_Scored']

    # drop duplicates based on 'First Name', 'Last Name', 'Zip', 'Debt Amount',
    # keeping the first occurrence after sorting by 'DM Reference ID'
    # this will remove duplicate rows and keep the first occurrence of each unique row
    # after sorting by 'DM Reference ID'
    other_df.drop_duplicates(subset=['First Name', 'Last Name', 'Zip', 'Debt Amount'], keep='first', inplace=True)

    # merge the two dataframes
    fixed_zipcode = pd.concat([other_df, to_be_scored_df])

    print('phase 2', fixed_zipcode.New_lead.value_counts())

    # replace NaN values in 'DM Reference ID' column with empty strings
    # this will ensure that the final dataframe has empty strings instead of NaN values
    # for the DM Reference ID column
    fixed_zipcode['DM Reference ID'].replace(np.nan, '', inplace=True)

    # We need to use this function to secure that all of our clients has an uuid
    # give_uuid_to_all_clients = give_prospects_a_unique_id_new(fixed_zipcode)

    # Create a new column named 'UUID' with a unique identifier for each row
    fixed_zipcode['UUID'] = [uuid.uuid4() for _ in range(len(fixed_zipcode))]

    # Create new dataframe with only the 'Name' and 'UUID' columns
    uuid_reference_id = fixed_zipcode[['UUID', 'DM Reference ID']]
    # from new dataframe keep only the rows that have a value in DM Reference ID
    uuid_reference_id = uuid_reference_id[uuid_reference_id['DM Reference ID'].str.isdigit()]

    # drop 'DM Reference ID' column
    df_without_reference_id = fixed_zipcode.drop(['DM Reference ID'], axis=1)
    rf.store_df_in_pickle_format(ConfigUtils.conf, prospects_pdr_files=df_without_reference_id,
                                 lead_reference_lookup=uuid_reference_id)

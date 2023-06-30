import pandas as pd
import reusable_functions as rf
from logger import Logger
from typing import Tuple
from Aggregate_data.config.config_utils import ConfigUtils
from Aggregate_data.config.__init__ import Config


def write_to_csv_leads_that_need_to_be_excluded_from_our_dataset(df: pd.DataFrame, checker: bool, conf: Config):
    # if checker is True this means that our leads have a Reference ID
    if checker:
        df.to_csv(conf.path_to_leads_with_id_that_need_to_be_excluded, sep=',',
                  encoding='utf-8', index=False)
    else:
        df.to_csv(conf.path_to_leads_without_id_that_need_to_be_excluded, sep=',',
                  encoding='utf-8', index=False)


def remove_prospects_with_reference_id(conf: Config, logger: Logger):
    initial_pdr_merged_df = rf.read_df_from_pickle_format(conf, 'initial_pdr_merged_df')
    opt_out_prospects_with_reference_id = rf.read_df_from_pickle_format(conf, 'with_reference_id_df')
    opt_out_prospects_with_reference_id.rename(columns={'Reference ID': 'DM Reference ID'}, inplace=True)
    opt_out_prospects_with_reference_id['DM Reference ID'] = opt_out_prospects_with_reference_id['DM Reference ID']. \
        apply(lambda x: rf.keep_number(x))

    filtered_df = initial_pdr_merged_df[
        ~initial_pdr_merged_df['DM Reference ID'].isin(opt_out_prospects_with_reference_id['DM Reference ID'])]

    excluded_df = initial_pdr_merged_df[
        initial_pdr_merged_df['DM Reference ID'].isin(opt_out_prospects_with_reference_id['DM Reference ID'])]

    write_to_csv_leads_that_need_to_be_excluded_from_our_dataset(excluded_df, True, ConfigUtils.conf)
    rf.store_df_in_pickle_format(ConfigUtils.conf, pdr_merged_df=filtered_df)


def remove_prospects_without_reference_id(conf, logger: Logger):
    """
     We are performing an inner join on the two dataframes, based on the four common columns. This will result in a new
     dataframe that only contains the rows where the values of the four columns match in both original dataframes. You
     can then drop these rows from one of the original dataframes to get the desired result.
    :param conf: conf = Config(debug_mode=True)
    :param logger: A parameter passed for testing purposes
    :return: A dataframe that does not contain prospects that asked to be excluded
    """

    opt_out_prospects_without_reference_id = rf.read_df_from_pickle_format(conf,
                                                                           'without_reference_id_df')
    initial_prospects_df = rf.read_df_from_pickle_format(conf, 'initial_prospects_df')

    # perform an inner join based on the four common columns
    merged = pd.merge(initial_prospects_df, opt_out_prospects_without_reference_id,
                      on=['First Name', 'Last Name', 'City', 'State'], how='inner')

    # We then use the isin() function to identify the matching rows in prospects_without_reference_id by comparing their
    # values in the four common columns with those in merged. We convert the rows to tuples using apply(tuple,1) to
    # allow for comparison, and use the ~ operator to invert the boolean mask that results from the comparison. Finally,
    # we use the resulting boolean mask to select the non-matching rows in prospects_without_reference_id.
    filtered_df = initial_prospects_df[~initial_prospects_df[
        ['First Name', 'Last Name', 'City', 'State']].apply(tuple, 1).isin(merged[['First Name', 'Last Name', 'City',
                                                                                   'State']].apply(tuple, 1))]
    excluded_df = initial_prospects_df[initial_prospects_df[
        ['First Name', 'Last Name', 'City', 'State']].apply(tuple, 1).isin(merged[['First Name', 'Last Name', 'City',
                                                                                   'State']].apply(tuple, 1))]

    write_to_csv_leads_that_need_to_be_excluded_from_our_dataset(excluded_df, False, ConfigUtils.conf)

    rf.store_df_in_pickle_format(ConfigUtils.conf, prospects_df=filtered_df)


def convert_all_values_in_upper_case(df: pd.DataFrame) -> pd.DataFrame:
    # convert all values in the "Address" column to uppercase
    df[['Address', 'First Name', 'Last Name', 'City']] = df[['Address', 'First Name', 'Last Name', 'City']].applymap(
        lambda x: x.upper() if isinstance(x, str) else x)

    return df


def split_into_2_dataframes(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the dataframe into two dataframes based on the Reference ID column by using the pandas.DataFrame.isnull
    method to create a boolean mask to filter the rows based on whether the Reference ID column is empty or not.
    :param df: The initial dataframe
    :return: df1: The dataframe with reference id, df2: The dataframe with empty value in reference id column
    """
    # filter rows based on Reference ID column
    mask = df['Reference ID'].isnull()
    # rows where Reference ID is not empty
    df1 = df[~mask]
    # rows where Reference ID is empty
    df2 = df[mask]

    return df1, df2


def clear_opt_out_list():
    opt_out_prospects = rf.read_excel_files(ConfigUtils.conf.path_opt_out_list, 'Mail-OptOut Supression List')
    with_reference_id_df, without_reference_id_df = split_into_2_dataframes(opt_out_prospects)
    without_reference_id_df = convert_all_values_in_upper_case(without_reference_id_df.copy())
    rf.store_df_in_pickle_format(ConfigUtils.conf, with_reference_id_df=with_reference_id_df,
                                 without_reference_id_df=without_reference_id_df)

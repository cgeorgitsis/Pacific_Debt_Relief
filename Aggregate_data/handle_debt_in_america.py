import pandas as pd
import numpy as np
from dotenv import dotenv_values
import reusable_functions as rf
from Aggregate_data.config.config_utils import ConfigUtils
from typing import Tuple


def take_mean_value_from_duplicate_columns(df: pd.DataFrame, feature: str) -> pd.DataFrame:
    """
    Calculates the mean value for each zipcode/ZCTA5 or each GEOID in each column of the input DataFrame.
    The NaN value will be ignored in the calculation of the mean due to the numeric_only=True parameter.
    :param df: The input DataFrame.
    :param feature: The feature based on which we group the rows and calculate the mean value of the other columns.
    :return:The resulting DataFrame with the mean values for each zipcode.
    """
    if feature == 'GEOID':
        # create a list of columns that are for sure strings
        string_cols = ['NAME', 'GEOID', 'state_name']
    else:
        # create a list of columns that are for sure strings
        string_cols = ['NAME', 'GEOID', 'state_name', 'ZCTA5']

    # create a list of all columns except for the string columns
    float_cols = [col for col in df.columns if col not in string_cols]

    # convert the float columns to string dtype
    df[float_cols] = df[float_cols].astype(str)

    # replace "n/a**" values with NaN
    df[float_cols] = df[float_cols].replace('n/a**', np.nan)
    df[float_cols] = df[float_cols].replace('n/a*', np.nan)

    # convert the float columns to float dtype
    df[float_cols] = df[float_cols].astype(float)

    # Merge the rows based on GEOID and take the mean value of other columns
    df = df.groupby([feature], as_index=False).mean(numeric_only=True)

    return df


def remove_suffixes_caused_by_merging_same_columns(df: pd.DataFrame) -> pd.DataFrame:
    # remove suffixes _x and _y from column names
    df.columns = df.columns.str.replace('_x', '').str.replace('_y', '')
    # Create a boolean mask of duplicate column names and Use the mask to select only the non-duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def merge_external_data(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, df4: pd.DataFrame) -> pd.DataFrame:
    merged_df = pd.merge(df1, df2, on='GEOID', how='left')
    merged_df = pd.merge(merged_df, df3, on='GEOID', how='left')
    merged_df = remove_suffixes_caused_by_merging_same_columns(merged_df)
    merged_df = pd.merge(merged_df, df4, on='GEOID', how='left')
    merged_df = remove_suffixes_caused_by_merging_same_columns(merged_df)
    return merged_df


def read_dataset_that_unifies_geoid_with_zipcodes() -> pd.DataFrame:
    env = dotenv_values()
    df = pd.read_csv(env['PATH_TO_DF_MATCHING_ZIPCODES_TO_GEOIDS'], dtype=str, delimiter=',')
    df = df.loc[:, ['ZCTA5', 'GEOID']]
    return df


def convert_geoid_column_to_str(df: pd.DataFrame) -> pd.DataFrame:
    df['GEOID'] = df['GEOID'].astype(str)
    return df


def read_file_names() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df1 = rf.read_excel_files(ConfigUtils.conf.path_debt_in_america_june_2022_auto, 'Sheet1')
    df2 = rf.read_excel_files(ConfigUtils.conf.path_debt_in_america_june_2022_delinquency, 'Sheet1')
    df3 = rf.read_excel_files(ConfigUtils.conf.path_debt_in_america_june_2022_medical, 'Sheet1')
    df4 = rf.read_excel_files(ConfigUtils.conf.path_debt_in_america_june_2022_student, 'Sheet1')
    return df1, df2, df3, df4


def format_debt_in_america_datasets(logger) -> Tuple[pd.DataFrame, str]:
    df1, df2, df3, df4 = read_file_names()

    # Create a dictionary of DataFrames
    dfs = {'df1': df1, 'df2': df2, 'df3': df3, 'df4': df4}
    # Loop over the dictionary and apply the function to each DataFrame
    for key, value in dfs.items():
        dfs[key] = convert_geoid_column_to_str(value)

    merge_debt_in_america = merge_external_data(df1, df2, df3, df4)
    final_debt_in_america = take_mean_value_from_duplicate_columns(merge_debt_in_america, 'GEOID')
    census_geo_data = read_dataset_that_unifies_geoid_with_zipcodes()
    merged_df = pd.merge(final_debt_in_america, census_geo_data, how='left', on='GEOID')
    merged_df = take_mean_value_from_duplicate_columns(merged_df, 'ZCTA5')
    merged_df.rename(columns={"ZCTA5": "input_feature_pd_customer_zip1"}, inplace=True)
    return merged_df, 'input_feature_pd_customer_zip1'

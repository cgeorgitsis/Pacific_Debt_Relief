import json
import os
import glob
import us

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from Aggregate_data.config.config_utils import ConfigUtils
from logger import Logger
from typing import Dict, Tuple


def create_column_containing_state_abbreviation(df: pd.DataFrame) -> pd.DataFrame:
    # Get the abbreviation of a state
    df['state_abbreviation'] = df['state'].apply(lambda x: us.states.lookup(x).abbr)
    df.drop(columns='state', axis=0, inplace=True)
    df.rename(columns={'state_abbreviation': 'input_feature_pd_customer_state'}, inplace=True)
    return df


def merge_all_philadelphia_bank_datasets() -> pd.DataFrame:
    path = ConfigUtils.conf.path_to_read_all_preprocessed_bank_of_philadelphia_files
    files = glob.glob(path)

    merged_df = None
    for file in files:
        try:
            # load the file into a pandas DataFrame
            df = pd.read_csv(file)

            # merge the current DataFrame with the merged DataFrame
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, how='left', on='state')
        except Exception as e:
            print(f"Error loading file {file}: {str(e)}")

    return merged_df


def store_csv(df: pd.DataFrame, file_name: str):
    # create the directory if it doesn't exist
    directory = ConfigUtils.conf.path_to_store_preprocessed_bank_of_philadelphia_files
    if not os.path.exists(directory):
        os.makedirs(directory)

    # replace spaces with underscores in file_name
    file_name = file_name.replace(' ', '_')

    # save the DataFrame to a CSV file
    file_path = os.path.join(directory, file_name)
    df.to_csv(file_path, index=False)


def keep_file_name_to_variable(path: str) -> str:
    json_basename = os.path.basename(path)
    # Get the parent directory name
    parent_dir = os.path.basename(os.path.dirname(path))
    # Get the file name without extension
    file_name = os.path.splitext(json_basename)[0]
    # Replace underscores with spaces and capitalize each word
    file_name = file_name.replace('_', ' ').title()
    # Combine parent directory name and file name
    csv_file_name = f'{parent_dir}_{file_name}.csv'

    return csv_file_name


def generate_column_names(file_path: str) -> Dict[str, str]:
    # Define a function to generate the new column names
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    dept_name = file_path.split('/')[-2].replace(" ", "_")
    return {
        'std': f'{dept_name}_{file_name}_std',
        'median': f'{dept_name}_{file_name}_median',
        'mean': f'{dept_name}_{file_name}_mean',
        'IQR': f'{dept_name}_{file_name}_IQR',
        'QR': f'{dept_name}_{file_name}_QR'
    }


def calculate_qr(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Calculates the range between the 1st and 100th percentile of the dollars column grouped by state.
    """
    q1 = df.groupby("state")[col_name].transform(lambda x: np.percentile(x, 25))
    q4 = df.groupby("state")[col_name].transform(lambda x: np.percentile(x, 100))
    df["QR"] = np.nan
    df["QR"] = np.where(df['group'] == 'total', q4 - q1, df['QR'])
    return df


def calculate_iqr(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Calculates the interquartile range (IQR).
    """
    q1 = df.groupby("state")[col_name].transform(lambda x: np.percentile(x, 25))
    q3 = df.groupby("state")[col_name].transform(lambda x: np.percentile(x, 75))
    df["IQR"] = np.nan
    df["IQR"] = np.where(df['group'] == 'total', q3 - q1, df['IQR'])
    return df


def calculate_mean(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Calculates the mean for rows where group is equal to "total".
    """
    df["mean"] = np.nan
    df.loc[df['group'] == 'total', 'mean'] = df.groupby("state")[col_name].transform(np.mean)
    return df


def calculate_median(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Calculates the median for rows where group is equal to "total".
    """
    df["median"] = np.nan
    df.loc[df['group'] == 'total', 'median'] = df.groupby("state")[col_name].transform(np.median)
    return df


def calculate_std(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Calculates the standard deviation for rows where group is equal to "total".
    """
    df["std"] = np.nan
    df.loc[df['group'] == 'total', 'std'] = df.groupby("state")[col_name].transform(np.std)
    return df


def fill_missing_values(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Fills missing values in the DataFrame with the mean.
    """
    df.loc[df[col_name].isna(), col_name] = df.groupby("state")[col_name].transform("mean")
    return df


def preprocess_and_add_statistic_columns(filtered_df: pd.DataFrame, column: str, file_path: str):
    df_fill_missing_values = fill_missing_values(filtered_df, column)
    df_fill_missing_values = df_fill_missing_values.copy()
    add_std_to_df = calculate_std(df_fill_missing_values, column)
    add_median_to_df = calculate_median(add_std_to_df, column)
    add_mean_to_df = calculate_mean(add_median_to_df, column)
    add_iqr_to_df = calculate_iqr(add_mean_to_df, column)
    add_qr_to_df = calculate_qr(add_iqr_to_df, column)

    # Drop duplicate rows based on specified columns and remove month column
    without_duplicates_df = add_qr_to_df.drop_duplicates(
        subset=['group', 'std', 'median', 'mean', 'IQR', 'QR']).drop('month', axis=1)

    # Generate the new column names based on the file name
    new_column_names = generate_column_names(file_path)

    # Rename the columns using the new names
    without_duplicates_df = without_duplicates_df.rename(columns=new_column_names)

    without_duplicates_df = without_duplicates_df.drop(['year', column, 'group'], axis=1)

    csv_file_name = keep_file_name_to_variable(file_path)

    store_csv(without_duplicates_df, csv_file_name)


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to keep only rows where group is equal to "total".
    """
    if 'group' in df.columns:
        df_filtered = df[df['group'] == 'total']
        return df_filtered
    else:
        raise KeyError('group column is missing in the DataFrame')


def get_dataframe_from_json_for_percent_change_in_aggregate_data(json_data: dict) -> pd.DataFrame:
    list_of_data = []
    for item in json_data['data']:
        state = item['state']
        # item['total'] = ['total', 'current', 'delinquent']
        for key in ['total', 'current', 'delinquent']:
            # Checks the total in outer scope
            if key in item:
                for t_key, t_value in item[key].items():
                    if 'total' in t_key:
                        for t in t_value:
                            if isinstance(t, dict):
                                list_of_data.append(
                                    {'state': state, 'month': t['month'], 'year': t['year'],
                                     'percentage': t['percentage'], 'group': f"{t_key}"})
                            else:
                                print(f"Invalid data type for {key} - {t_key}: {type(t)}")
    df = pd.DataFrame(list_of_data)
    return df


def get_dataframe_from_json_for_credit_constrained_consumers(json_data: dict) -> pd.DataFrame:
    """
    Converts a dictionary object to a pandas DataFrame.
    """
    list_of_data = []
    for item in json_data['data']:
        state = item['state']
        for t in item['total']:
            list_of_data.append(
                {'state': state, 'month': t['month'], 'year': t['year'], 'percentage': t['percentage'],
                 'group': 'total'})
        for group, v in item['neighborhood_income_groups'].items():
            for g in v:
                list_of_data.append(
                    {'state': state, 'month': g['month'], 'year': g['year'], 'percentage': g['percentage'],
                     'group': group})
        for group, v in item['age_groups'].items():
            for g in v:
                list_of_data.append(
                    {'state': state, 'month': g['month'], 'year': g['year'], 'percentage': g['percentage'],
                     'group': group})
    df = pd.DataFrame(list_of_data)
    return df


def get_dataframe_from_json_for_home_equity(json_data: dict) -> pd.DataFrame:
    """
    Reads a JSON file and returns the data as a pandas DataFrame.
    """

    df_data = []
    for item in json_data['data']:
        state = item['state']
        df_data += extract_total_data(state, item)
    return pd.DataFrame(df_data)


def extract_total_data(state: str, data: dict) -> list:
    list_of_data = []
    for t in data['total']:
        if 'dollars' in t:
            list_of_data.append(
                {'state': state, 'month': t['month'], 'year': t['year'], 'dollars': t['dollars'], 'group': 'total'})
        elif 'percentage' in t:
            list_of_data.append(
                {'state': state, 'month': t['month'], 'year': t['year'], 'percentage': t['percentage'],
                 'group': 'total'})
    return list_of_data


def extract_group_data(state: str, group_name: str, data: dict) -> list:
    list_of_data = []
    for group_key, values in data[group_name].items():
        for v in values:
            if 'dollars' in v:
                list_of_data.append({'state': state, 'month': v['month'], 'year': v['year'], 'dollars': v['dollars'],
                                'group': group_key})
            elif 'percentage' in v:
                list_of_data.append(
                    {'state': state, 'month': v['month'], 'year': v['year'], 'percentage': v['percentage'],
                     'group': group_key})
    return list_of_data


def get_dataframe_from_json(json_data: dict) -> pd.DataFrame:
    """
    Converts a dictionary object to a pandas DataFrame.
    """
    df_data = []
    for item in json_data['data']:
        state = item['state']
        df_data.extend(extract_total_data(state, item))
        df_data.extend(extract_group_data(state, 'neighborhood_income_groups', item))
        df_data.extend(extract_group_data(state, 'age_groups', item))
        df_data.extend(extract_group_data(state, 'credit_score_groups', item))
    df = pd.DataFrame(df_data)
    return df


def load_json_file(file_path: str) -> dict:
    """
    Loads a JSON file and returns the contents as a dictionary object.
    """
    with open(file_path, 'r') as f:
        return json.load(f)


def format_federal_reserve_bank_philadelphia_dataset(logger: Logger) -> Tuple[pd.DataFrame, str]:
    load_dotenv()
    path = ConfigUtils.conf.path_federal_reserve_bank_philadelphia

    for file_path in glob.glob(path):
        dictionary = load_json_file(file_path)
        if 'Home Equity Line of Credit' in file_path:
            df = get_dataframe_from_json_for_home_equity(dictionary)
        elif 'Credit Constrained Consumers' in file_path:
            df = get_dataframe_from_json_for_credit_constrained_consumers(dictionary)
        elif 'percent_change_in_aggregate_debt' in file_path:
            df = get_dataframe_from_json_for_percent_change_in_aggregate_data(dictionary)
        else:
            df = get_dataframe_from_json(dictionary)

        filtered_df = filter_dataframe(df)

        if 'dollars' in filtered_df.columns:
            preprocess_and_add_statistic_columns(filtered_df, 'dollars', file_path)
        elif 'percentage' in filtered_df.columns:
            preprocess_and_add_statistic_columns(filtered_df, 'percentage', file_path)

    merged_df = merge_all_philadelphia_bank_datasets()

    merged_df = create_column_containing_state_abbreviation(merged_df)

    merged_df.to_csv(ConfigUtils.conf.path_to_store_bank_of_philadelphia_final_file,
                     sep=',', encoding='utf-8', index=False)
    return merged_df, 'input_feature_pd_customer_state'

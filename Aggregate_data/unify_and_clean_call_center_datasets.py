import glob

import pandas as pd

import reusable_functions as rf
from Aggregate_data.config.config_utils import ConfigUtils
from logger import Logger
from typing import Dict


def read_inbound_format_1() -> pd.DataFrame:

    file_paths = glob.glob(ConfigUtils.conf.call_center_inbound_path)

    dfs_to_concat = []
    for file_path in file_paths:
        for sheet_name in pd.read_excel(file_path, sheet_name=None).keys():
            if sheet_name not in ['Cover Sheet', 'Call Distribution', 'Call Times']:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                dfs_to_concat.append(df)

    # Concatenate all the dataframes into a single dataframe
    concatenated_df = pd.concat(dfs_to_concat).reset_index(drop=True)

    keep_specific_columns = concatenated_df.loc[:, ['Date', 'Queue', 'Trunk', 'Caller ID', 'Call Time', 'Exit Reason']]

    keep_specific_columns['CRM Status'] = ''

    final_df = final_preprocessing(keep_specific_columns)
    final_df['Trunk'] = final_df['Trunk'].astype(str)

    return final_df


def preprocess_df_1(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = ['#', 'Date', 'Queue', 'Trunk', 'Caller ID', 'Agent', 'Wait', 'Call Time', 'Exit Reason',
                  'CRM Status']
    # Bring the tables in the same format
    df = df.drop(['Agent', 'Wait'], axis=1)
    # check if any row is equal to column names
    equal_to_colnames = (df == df.columns).all(axis=1)
    # drop the row where all values are equal to column names
    df = df[~equal_to_colnames]

    return df


def preprocess_the_other_dfs(df: pd.DataFrame, column_names: Dict[str, str]) -> pd.DataFrame:
    df.columns = ['#', 'Date', 'Queue', 'Trunk', 'Source', 'Destination', 'Caller ID', 'Call Time', 'Exit Reason',
                  'CRM Status']
    # Define the conditions for dropping a row
    conditions = (df['#'] == '#') & \
                 (df['Date'] == 'Date') & \
                 (df['Queue'] == 'Direction') & \
                 (df['Trunk'] == 'Trunk') & \
                 (df['Caller ID'] == 'Caller ID') & \
                 (df['Call Time'] == 'Call Time') & \
                 (df['Exit Reason'] == 'Disposition') & \
                 (df['CRM Status'] == 'CRM Status')

    # Drop the row(s) that match the conditions
    df = df[~conditions]
    df = df.rename(columns=column_names)

    return df.drop(['Source', 'Destination'], axis=1)


def final_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """
    "To extract only the 'CRM Status' column, it is important to replace all punctuation marks except '-' with a space
    character. Otherwise, pandas may split the column into multiple columns at each occurrence of a punctuation mark,
    resulting in unexpected behavior. By replacing the punctuation marks with spaces, we ensure that the 'CRM Status'
    column contains only the relevant information and does not get split into multiple columns."
    """
    df['CRM Status'] = df['CRM Status'].str.replace('[^\w\s-]', ' ', regex=True)

    # Further preprocessing steps
    # Step 1: Drop all 3-digits (internal calls within a company. Does not add value to our data).
    df = df[df['Caller ID'].astype(str).str.len() != 3].copy()

    # Step 2: When we have an 11-digit number we need to drop the first digit
    df.loc[:, 'Caller ID'] = df['Caller ID'].astype(str).str.replace(r'^\d(\d{10})$', r'\1', regex=True).copy()

    # Step3: Convert this number 999999999 to 0999999999
    df.loc[:, 'Caller ID'] = df['Caller ID'].astype(str).str.zfill(10).copy()

    # Step 4 Keep only numbers that are 10-digits
    df = df[df['Caller ID'].astype(str).str.len() == 10].copy()

    return df


def clean_call_center_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Each sheet pertaining to an agent is categorized into five sections: Agent Performance/Queue, Details-Queue Calls,
    Details-Inbound Calls, Details-Outbound Calls, and Details-Internal Calls. However, we exclude the first category as
    it does not provide any valuable information to our dataset. We then create four distinct tables for the remaining
    categories and perform preprocessing steps to standardize their formats. This allows us to concatenate the four
    tables into a single dataframe.
    :param df: The sheet that contains information concerning a specific agent
    :return: A single dataframe
    """
    # Find the index of the first occurrence of 'Agent Performance / Queue' in column A
    index_to_drop = df.loc[df['Agent Performance / Queue'] == 'Details - Queue Calls'].index[0]
    # Drop all rows from the beginning of the dataframe up to the row found above
    df = df.drop(range(index_to_drop))

    # Drop all rows and columns with empty cells
    df = df.dropna(how='all').dropna(axis=1, how='all')

    tables = []
    table_names = ['Details - Queue Calls', 'Details - Inbound Calls', 'Details - Outbound Calls',
                   'Details - Internal Calls']

    for i in range(len(table_names)):
        start_index = df.loc[df['Agent Performance / Queue'] == table_names[i]].index[0]
        if i == len(table_names) - 1:
            end_index = df.index[-1]
        else:
            end_index = df.loc[df['Agent Performance / Queue'] == table_names[i + 1]].index[0] - 1
        tables.append((start_index, end_index))

    # Access the first table
    table_1_start_index, table_1_end_index = tables[0][0], tables[0][1]
    table_2_start_index, table_2_end_index = tables[1][0], tables[1][1]
    table_3_start_index, table_3_end_index = tables[2][0], tables[2][1]
    table_4_start_index, table_4_end_index = tables[3][0], tables[3][1]

    # Rename all the existing column names
    df.columns = [f'col_{i}' for i in range(len(df.columns))]

    df_1 = df.loc[table_1_start_index:table_1_end_index].reset_index(drop=True)
    df_2 = df.loc[table_2_start_index:table_2_end_index].reset_index(drop=True)
    df_3 = df.loc[table_3_start_index:table_3_end_index].reset_index(drop=True)
    df_4 = df.loc[table_4_start_index:table_4_end_index].reset_index(drop=True)

    df_1 = preprocess_df_1(df_1)
    df_2 = preprocess_the_other_dfs(df_2, {'Source': 'Caller ID', 'Caller ID': 'Source'})
    df_3 = preprocess_the_other_dfs(df_3, {'Destination': 'Caller ID', 'Caller ID': 'Destination'})
    df_4 = preprocess_the_other_dfs(df_4, {'Source': 'Caller ID', 'Caller ID': 'Source'})

    df_list = [df_1, df_2, df_3, df_4]
    new_df = pd.concat(df_list)

    # create a list of values to drop
    values_to_drop = ['#', 'Details - Internal Calls', 'Details - Outbound Calls',
                      'Details - Inbound Calls', 'Details - Queue Calls']
    # drop the rows that have the specified values in the specified column named '#'
    new_df = new_df[~new_df['#'].isin(values_to_drop)]

    return new_df


def read_call_center_data() -> pd.DataFrame:
    call_center_paths = glob.glob(ConfigUtils.conf.call_center_path)
    call_center = []

    for path in call_center_paths:
        fullname_prospect_df = rf.read_excel_files(path, None)

        exclude_sheets_names = ['Cover Sheet', 'Agent Performance']
        keep_sheet_names = fullname_prospect_df.keys() - exclude_sheets_names

        dfs = {sheet_name: fullname_prospect_df[sheet_name] for sheet_name in keep_sheet_names}

        results = []
        for sheet_name, df in dfs.items():
            new_df = clean_call_center_data(df)
            results.append(new_df)

        # concatenates all the cleaned dataframes for each sheet within a single Excel file and returns
        # a single dataframe containing all the rows from those sheets.
        final_df = pd.concat(results)
        # after the loop completes, call_center will contain a list of dataframes, where each dataframe represents
        # the cleaned data from a single Excel file.
        call_center.append(final_df)

    call_centers_df = pd.concat(call_center)
    call_centers_df.drop(columns=['#'], axis=1, inplace=True)
    return call_centers_df


def format_call_center_dataset(logger: Logger):
    call_center_df = read_call_center_data()
    four_sources_df = final_preprocessing(call_center_df)
    keep_only_columns_needed = four_sources_df.loc[:, ['Date', 'Queue', 'Trunk', 'Caller ID', 'Call Time',
                                                       'Exit Reason', 'CRM Status']]
    inbound_df = read_inbound_format_1()
    final_list = [keep_only_columns_needed, inbound_df]
    final_df = pd.concat(final_list).reset_index(drop=True)
    final_df = final_df.drop_duplicates()
    final_df.to_csv(ConfigUtils.conf.path_final_call_center, sep=',', encoding='utf-8', index=False)

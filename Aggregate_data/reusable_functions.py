import pandas as pd
import re, pickle, os
from Aggregate_data.config.__init__ import Config


def remove_all_columns_of_object_type(df):
    """
    This function removes all columns from the dataframe of type object
    """
    # select only columns that have a non-object data type (i.e., numeric or boolean)
    df = df.select_dtypes(exclude=['object'])
    # drop the selected columns from the original dataframe
    df = df.drop(columns=df.select_dtypes(include=['object']).columns)

    return df


def keep_number(x):
    return "".join(re.sub("[^0-9]", "", str(x)))


def keep_letters(x):
    return re.sub(r'[^a-zA-Z]', '', str(x))


def check_number_of_digits(df):
    """
    Description: Keeps only the clients with a 10-digit id
    :param df: The given dataframe
    :return: A new dataframe containing only the clients with 10-digit id
    """
    pattern = re.compile(r'^\d{10}$')
    mask = df['DM Reference ID'].astype(str).str.match(pattern)
    new_df = df[mask]
    return new_df


def read_excel_files(file, sheet):
    return pd.read_excel(file, sheet_name=sheet)


def read_csv_file(file: str, **kwargs):
    return pd.read_csv(file, sep=',', low_memory=False, **kwargs)


def store_df_in_pickle_format(conf: Config, **kwargs):
    # Create the directory if it doesn't exist
    if not os.path.exists(conf.path_to_store_pickle_files):
        os.makedirs(conf.path_to_store_pickle_files)

    # Store the dataframes as pickled objects in the directory
    for name, df in kwargs.items():
        with open(os.path.join(conf.path_to_store_pickle_files, f'{name}.pickle'), 'wb') as f:
            pickle.dump(df, f)


def read_df_from_pickle_format(conf: Config, pickle_name):
    with open(os.path.join(conf.path_to_store_pickle_files, f'{pickle_name}.pickle'), 'rb') as f:
        df = pickle.load(f)
    return df

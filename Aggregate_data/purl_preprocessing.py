import pandas as pd
import os
import reusable_functions as rf


def preprocess_merge_df(df1, df2):
    new_df = pd.merge(df1, df2, on='DM Reference ID', how='left')
    new_df.drop(['Lead Source_y', 'Zip_y', 'Debt Amount_y'], axis=1, inplace=True)
    new_df.rename({'Lead Source_x': 'Lead Source', 'Zip_x': 'Zip', 'Debt Amount_x': 'Debt Amount'},
                  axis=1, inplace=True)

    # Step_1: keep only digits from df['DM Reference ID']
    new_df['DM Reference ID'] = new_df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    # Step_2: check if the df['DM_Reference_ID'] is 10 digits else drop it
    temp_df = rf.check_number_of_digits(new_df)
    df = temp_df
    df['DM Reference ID'] = df['DM Reference ID'].astype('str')
    has_valid_len = (df['DM Reference ID'].astype(str).str.len() == 10).all()
    return df


def clear_dataset(df):
    df['DM Reference ID'] = df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    df['Zip'] = df['Zip'].apply(lambda x: rf.keep_number(x))
    df.dropna(subset=['DM Reference ID'], inplace=True)
    purl_dataset = df.drop_duplicates(subset=['DM Reference ID', 'Zip'], keep='last').copy()
    purl_dataset.drop(['DM PURL'], axis=1, inplace=True)
    purl_dataset = purl_dataset.loc[:, ['DM Reference ID', 'Lead Source', 'Zip', 'Debt Amount']]
    return purl_dataset


def read_csv_file():
    return rf.read_csv_file(os.environ.get('PATH_PURL'))


def format_purl_dataset(pdr_df):
    purl_df = read_csv_file()
    clean_df = clear_dataset(purl_df)
    rf.check_number_of_digits(clean_df)
    return preprocess_merge_df(pdr_df, clean_df)

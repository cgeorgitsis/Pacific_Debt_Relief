import pandas as pd
import os
import glob
import pickle
import re


def keep_number(x):
    return "".join(re.sub("[^0-9]", "", str(x)))


def clear_reference_id(df: pd.DataFrame) -> pd.DataFrame:
    df['DM Reference ID'] = df['DM Reference ID'].apply(lambda x: keep_number(x))
    return df


def unzip_zipcode(pdr_file: pd.DataFrame) -> pd.DataFrame:
    pdr_file['Zip'] = pdr_file['Zip'].astype(str).str.zfill(9)
    pdr_file = pdr_file.assign(Zip1=pdr_file['Zip'].astype(str).str[:5], Zip2=pdr_file['Zip'].astype(str).str[5:])
    pdr_file = pdr_file.drop(['Zip'], axis=1)
    pdr_file['Zip2'] = pdr_file['Zip2'].str.replace('-', '')
    return pdr_file


def read_df_from_pickle_format():
    with open('../Datasets/Output_datasets/Pickle_files/pdr_merged_df.pickle', 'rb') as f:
        df = pickle.load(f)
    return df


def merge_scored_prospects(df1, df2):
    column_mapping = {
        'FNAME': 'First Name',
        'LNAME': 'Last Name',
        'EST DEBT': 'Debt Amount',
        'ZIP': 'Zip1',
        'ZIP4': 'Zip2',
    }
    df1.rename(columns=column_mapping, inplace=True)

    merge_columns = ['First Name', 'Last Name', 'Debt Amount', 'Zip1', 'Zip2']
    # columns_to_keep_df2 = [col for col in df2.columns if col not in merge_columns and
    #                        col != ['mailing_date', 'Ranking', 'Decision']]
    merged_df = df1.merge(df2, how='inner', left_on=merge_columns, right_on=merge_columns)

    # Drop the duplicate columns (columns from df2 that are the same as in df1)
    # merged_df.drop(columns_to_keep_df2, axis=1, inplace=True)

    columns_to_drop = ['MI', 'SUFFIX']
    merged_df.drop(columns_to_drop, axis=1, inplace=True)
    # Drop duplicates based on all columns except 'mailing_date'
    merged_df = merged_df.drop_duplicates(subset=merged_df.columns.difference(['Ranking', 'mailing_date', 'Decision']))

    return merged_df


def rank_all_prospects(path, col_name, ranking_column):
    file_pattern = '*.csv'
    file_path_pattern = os.path.join(path, file_pattern)
    csv_files = glob.glob(file_path_pattern)

    dataframes = []

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        df = pd.read_csv(file_path)

        date_str = file_name.split(' ')[0][2:]
        mailing_date = pd.to_datetime(date_str, format='%m%d%y')
        df[col_name] = mailing_date

        if ranking_column:
            percentile_98_percent = df['Ranking'].quantile(0.98)
            # Create a column named 'Decision' with a value of 'Mail' if the Rank is not in the bottom 2%
            df['Decision'] = df['Ranking'].apply(lambda rank: 'Mail' if rank < percentile_98_percent else 'Do not mail')

        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    no_duplicates_df = combined_df.drop_duplicates()
    return no_duplicates_df


if __name__ == "__main__":
    to_be_ranked_prospects = rank_all_prospects(path='../Datasets/To_Be_Scored',
                                                col_name='scoring_date', ranking_column=False)
    ranked_prospects = rank_all_prospects(path='../Datasets/Scored_Data', col_name='mailing_date', ranking_column=True)
    ranked_prospects_more_info = merge_scored_prospects(to_be_ranked_prospects, ranked_prospects)
    column_mapping = {
        'ADDRESS': 'Address',
        'CITY': 'City',
        'STATE': 'State',
    }

    ranked_prospects_more_info.rename(columns=column_mapping, inplace=True)
    ranked_prospects_more_info['Zip1'] = ranked_prospects_more_info['Zip1'].astype(str)
    ranked_prospects_more_info['Zip2'] = ranked_prospects_more_info['Zip2'].astype(str)
    ranked_prospects_more_info['Debt Amount'] = ranked_prospects_more_info['Debt Amount'].astype(float)

    pdr_prospects = read_df_from_pickle_format()
    pdr_prospects.drop(['Lead Source', 'Direct Mail DID', 'DM PURL', 'gender', 'Mail_number'], axis=1, inplace=True)
    pdr_prospects_with_unzipped_zipcode = unzip_zipcode(pdr_prospects)

    merged_df = pd.merge(ranked_prospects_more_info, pdr_prospects_with_unzipped_zipcode,
                         on=['First Name', 'Last Name', 'Zip1', 'Zip2', 'Debt Amount', 'Address', 'City', 'State'],
                         how='left')
    merged_df = merged_df.drop_duplicates(subset=merged_df.columns.difference(['DM Reference ID']))
    merged_df = clear_reference_id(merged_df)

    status_df = pd.read_csv('../Datasets/SabinoDB-DMIngestion/new_Status/SabinoDB-DMIngestion-Report.csv', sep=',')
    status_df.drop(['Id', 'Lead Source', 'Home Phone', 'Mobile Phone', 'Work Phone'], axis=1, inplace=True)
    status_df = clear_reference_id(status_df)

    merged_df.to_csv('all_mailed_prospects.csv', sep=',', encoding='utf-8', index=False)

    filtered_merged_df = merged_df[merged_df['DM Reference ID'] != '']
    filtered_status_df = status_df[status_df['DM Reference ID'] != '']

    final_df = pd.merge(filtered_merged_df, filtered_status_df, on='DM Reference ID', how='left')
    final_df['Converted_to_Client'] = final_df['Status'].isin(['Client', 'C1 Client']).astype(int)
    final_df.to_csv('mailed_prospects.csv', sep=',', encoding='utf-8', index=False)

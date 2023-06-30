import glob
import gender_guesser.detector as gender
import numpy as np
import pandas as pd
import reusable_functions as rf
from dotenv import load_dotenv
from Aggregate_data.config.config_utils import ConfigUtils
from typing import Tuple


def map_gender(g: str):
    if g == 'mostly_female':
        return 'female'
    elif g == 'mostly_male':
        return 'male'
    else:
        return g


def create_gender_column(df: pd.DataFrame) -> pd.DataFrame:
    # The difference between andy and unknown is that the former is found to have the same probability to be male than
    # to be female, while the later means that the name wasn’t found in the database.
    # create a gender detector object
    detector = gender.Detector(case_sensitive=False)
    detector._THRESHOLD_RATIO = 0.9
    # apply the get_gender function to the 'First name' column and store the result in a new 'gender' column
    # It needs this check --> isinstance because we take a float somewhere in First Name column
    df['gender'] = df['First Name'].apply(
        lambda x: map_gender(detector.get_gender(x)) if isinstance(x, str) else np.nan)
    return df


def create_mail_number_column(new_df: pd.DataFrame) -> pd.DataFrame:
    new_df = new_df.copy()
    # If we do not reset the index we will take an error
    new_df = new_df.reset_index(drop=True)
    # We keep the email ending in 'B' or 'b' in case of duplicate emails. This method enables us to accurately monitor
    # the number of emails that each lead has received.
    new_df = new_df.drop_duplicates(subset=['DM Reference ID', 'Zip'], keep='last')
    # Create the new column (Mail_number) based on the letter in 'DM Reference ID'
    new_df['Mail_number'] = new_df['DM Reference ID'].apply(lambda x: rf.keep_letters(x))
    new_df.loc[new_df["Mail_number"] == "A", "Mail_number"], new_df.loc[
        new_df["Mail_number"] == "B", "Mail_number"] = 1, 2
    return new_df


def clear_dataset(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # This feature (Status) does not have any relationship with the feature Status that exist in other datasets
    list1 = [df1, df2]
    new_list = []
    for df in list1:
        df.drop(['Status'], axis=1, inplace=True)
        df = create_mail_number_column(df)
        df['DM Reference ID'] = df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
        df.drop_duplicates(subset=['DM Reference ID', 'Zip'], keep='last')
        df['DM Reference ID'].replace('', np.nan, inplace=True)
        df.dropna(subset=['DM Reference ID'], inplace=True)
        # Check if the df_merged_datasets['DM_Reference_ID'] is 10 digits else drop it
        new_df = rf.check_number_of_digits(df)
        new_list.append(new_df)
    return new_list[0], new_list[1]


def concatenate_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    # In order to concatenate two dataframes we need to first convert them in a List or a dict
    frames = [df1, df2]
    result = pd.concat(frames, axis=0)
    # This column drop duplicates with the same values in columns 'DM Reference ID', 'Zip'
    # by keeping the max value of Mail_number column
    result = result.sort_values(by=['Mail_number'], ascending=False).drop_duplicates(subset=['DM Reference ID', 'Zip'],
                                                                                     keep='first')
    return result


def format_pdr_file_dataset():

    # Load the environment variables
    load_dotenv()
    # exclude xlsx like:
    # /home/user/DEV/databreathe-pacific-dept/Datasets/Raw_Data/PDR_Files/040323/47247_PD_DM_D19_T1_HIF_CMGL.xlsx
    path = ConfigUtils.conf.pdr_files_path

    # Create an empty list to store the dataframes
    pdr_dfs = []

    # Loop through all the Excel files found
    for file in glob.glob(path, recursive=True):
        # Check if Sheet4 exists in the Excel file
        if 'Sheet4' in pd.ExcelFile(file).sheet_names:
            # Read the Excel file with sheet_name='Sheet4'
            df = rf.read_excel_files(file, 'Sheet4')
            # Check if the sheet is empty
            if df.empty:
                # If it is empty, skip to the next file
                continue

            # Convert all files to same format
            df.rename(columns={"Reference ID": "DM Reference ID", "Zip Code": 'Zip', "Surname": "Last Name",
                               "Street": "Address", "State Abbreviation": "State", "EHV": "Debt Amount",
                               "UTL": "Debt Amount", "DID": "Direct Mail DID"}, inplace=True)
            df = df.loc[:, ['Lead Source', 'Status', 'First Name', 'Last Name', 'Address', 'City', 'State', 'Zip',
                            'Debt Amount', 'DM Reference ID', 'Direct Mail DID', 'DM PURL']]
            pdr_dfs.append(df)

    # Concatenate all the dataframes into a single dataframe
    pdr_df = pd.concat(pdr_dfs, axis=0, ignore_index=True)
    pdr_df = create_gender_column(pdr_df)
    pdr_df = create_mail_number_column(pdr_df)
    pdr_df = pdr_df.drop(['Status'], axis=1)
    pdr_df = pdr_df.drop_duplicates(subset=['DM Reference ID', 'Zip'], keep='last')
    pdr_df['DM Reference ID'] = pdr_df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    pdr_df['DM Reference ID'].replace('', np.nan, inplace=True)
    pdr_df.dropna(subset=['DM Reference ID'], inplace=True)
    pdr_df = rf.check_number_of_digits(pdr_df)
    pdr_df = pdr_df.sort_values(by=['Mail_number'], ascending=False).drop_duplicates(subset=['DM Reference ID', 'Zip'],
                                                                                     keep='first')
    rf.store_df_in_pickle_format(ConfigUtils.conf, initial_pdr_merged_df=pdr_df)


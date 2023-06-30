import re, os, glob
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import reusable_functions as rf
import gender_guesser.detector as gender
from Aggregate_data.config.config_utils import ConfigUtils


def reshape_prospects_df_into_pdr(df: pd.DataFrame) -> pd.DataFrame:
    # add a new column named "Lead Source"
    df['Lead Source'] = ''
    # add a new column named "DM Reference ID"
    df['DM Reference ID'] = ''
    df['Direct Mail DID'] = ''
    df['DM PURL'] = ''
    # add a new column named "Mail_number" with all values set to 0
    df['Mail_number'] = 0

    # convert zip to this form 11111-1111
    df['Zip'] = [f"{zip1}-{zip2}" for zip1, zip2 in zip(df['Zip_1'], df['Zip_2'])]

    # Rename columns
    df = df.rename(columns={'FNAME': 'First Name', 'LNAME': 'Last Name', 'ADDRESS': 'Address', 'CITY': 'City',
                            'STATE': 'State', 'EST DEBT': 'Debt Amount'})

    # Specify the new column order
    new_order = ['Lead Source', 'Lead purchased', 'First Name', 'Last Name', 'Address', 'City', 'State', 'Zip',
                 'Debt Amount',
                 'DM Reference ID', 'Direct Mail DID', 'DM PURL', 'gender', 'Mail_number', 'New_lead']

    # Reindex the DataFrame with the new column order
    df = df.reindex(columns=new_order)

    return df


def map_gender(g: str) -> str:
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
    df.loc[:, 'gender'] = df['FNAME'].apply(
        lambda x: map_gender(detector.get_gender(x)) if isinstance(x, str) else np.nan)
    return df.copy()


def drop_all_new_leads_that_already_exist_to_our_df(df: pd.DataFrame) -> pd.DataFrame:
    # Identify duplicates based on the columns 'FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2'
    duplicates = df.duplicated(subset=['FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2'], keep=False)

    # Keep the rows that are not duplicates or those that have value == 'to_be_Scored' for duplicates.
    umg_prospects_df = df[~duplicates | (df['New_lead'] == 'to_be_Scored')]

    # Create a copy of the dataframe and drop duplicates based on 'FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2'
    umg_prospects_df_copy = umg_prospects_df.drop_duplicates(
        subset=['FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2'], keep='first')

    # Drop rows with '' in the 'New_lead' column, keeping the ones with an empty string ''
    umg_prospects_df_copy = umg_prospects_df_copy.loc[
        ~umg_prospects_df_copy[['FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2']].duplicated(keep=False) | (
                umg_prospects_df_copy['New_lead'] == 'to_be_Scored')]

    return umg_prospects_df_copy


def keep_record_of_new_leads_that_already_exist_to_previous_datasets(df: pd.DataFrame):
    """
    The duplicated() method is used to identify the rows that are duplicates based on the specified columns
    ('FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2'). keep=False means that all duplicates are marked as True,
    and the first occurrence of each duplicate is marked as False. The resulting boolean series is then combined
    with another condition using the & operator to filter only the rows where the value in the 'New_lead' column is
    equal to 'to_be_Scored'.
    """
    # filter the rows
    duplicates_to_score = df.duplicated(subset=['FNAME', 'LNAME', 'EST DEBT', 'CITY', 'Zip_1', 'Zip_2'],
                                        keep=False) & (df['New_lead'] == 'to_be_Scored')

    # save the filtered DataFrame to a CSV file
    duplicates_to_score_df = df[duplicates_to_score]
    duplicates_to_score_df.to_csv('../Datasets/Output_datasets/duplicates_to_score.csv', index=False)


def create_column_new_lead(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    # Check if the file path contains the string 'To Be Scored'
    if "To Be Scored" in os.path.abspath(file_path):
        new_lead = "to_be_Scored"
    else:
        new_lead = ""

    # Add a new column to the DataFrame with the new_lead value
    df['New_lead'] = new_lead

    return df


def create_column_date_purchased_new_lead(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    # The regular expression pattern matches any sequence of six digits preceded by an underscore and followed by a
    # non-digit character or the end of the string.
    date_regex = r"(?<=_)\d{6}(?=\D|$)"
    match = re.search(date_regex, filename)

    if match:
        date_str = match.group()
        df['Lead purchased'] = pd.to_datetime(date_str, format='%m%d%y')
    else:
        df['Lead purchased'] = pd.NaT

    return df


def clean_prospect_datasets(df: pd.DataFrame, file_path: str, filename: str) -> pd.DataFrame:
    new_df = create_column_new_lead(df, file_path)

    if 'EST DEBT ' in list(new_df.columns):
        new_df.rename(columns={'EST DEBT ': 'EST DEBT'}, inplace=True)
    new_df['ZIP'] = new_df['ZIP'].astype(str).str.zfill(5)
    new_df['ZIP4'] = new_df['ZIP4'].astype(str).str.zfill(4)
    new_df = new_df.rename(columns={'ZIP': 'Zip_1', 'ZIP4': 'Zip_2'})

    # Add the date the lead was purchased to the DataFrame
    new_df = create_column_date_purchased_new_lead(new_df, filename)

    return new_df


def split_zip_code_into_2_columns(df: pd.DataFrame) -> pd.DataFrame:
    # fill with zeros until ZIP column reaches 9 digits
    df['ZIP'] = df['ZIP'].astype(str).str.zfill(9)
    df = df.assign(Zip_1=df['ZIP'].astype(str).str[:5], Zip_2=df['ZIP'].astype(str).str[5:])
    df = df.drop(['ZIP'], axis=1)
    df['Zip_2'] = df['Zip_2'].str.replace('-', '')
    return df


def clean_fullname_datasets(new_df: pd.DataFrame, filename: str) -> pd.DataFrame:
    if 'utilization' in list(new_df.columns):
        new_df.rename(columns={'utilization': 'UTL'}, inplace=True)
    if 'debt' in list(new_df.columns):
        new_df.rename(columns={'debt': 'EST DEBT'}, inplace=True)
    new_df = new_df.drop('UTL', axis=1)
    new_df = new_df.rename(columns={'First Name': 'FNAME', 'Middle Initial': 'MI', 'Surname': 'LNAME',
                                    'Gen Code': 'SUFFIX', 'Street': 'ADDRESS', 'City': 'CITY',
                                    'State Abbreviation': 'STATE', 'Zip Code': 'ZIP', 'EHV': 'EST DEBT'})
    new_df = split_zip_code_into_2_columns(new_df)

    # Add the date the lead was purchased to the DataFrame
    new_df = create_column_date_purchased_new_lead(new_df, filename)

    return new_df


def format_umg_datasets():
    """"
    The files we received follow 2 different formats (different column names and some minor differences in
    column's content). For this reason we need to handle them differently, before we concatenate them to a unified df
    """
    load_dotenv()

    fullname_prospect_files = glob.glob(ConfigUtils.conf.prospect_fullname_path, recursive=True)
    to_be_scored_files = glob.glob(ConfigUtils.conf.prospects_to_be_scored_path)
    prospect_files = glob.glob(ConfigUtils.conf.prospect_path, recursive=True)
    # remove the files that match the fullname pattern
    prospect_files = list(set(prospect_files) - set(fullname_prospect_files) - set(to_be_scored_files))

    umg_prospects = []
    for file_path in fullname_prospect_files:
        key = 'PATH_PROSPECTS_FULLNAME_' + file_path.split('/')[-1].split('.')[0].upper()
        fullname_prospect_df = rf.read_csv_file(file_path)
        cleaned_fullname_prospect_df = clean_fullname_datasets(fullname_prospect_df, key)
        umg_prospects.append(cleaned_fullname_prospect_df)

    for file_path in to_be_scored_files:
        key = 'PATH_PROSPECTS_PD_' + file_path.split('/')[-1].split('.')[0].upper()
        prospect_df = rf.read_csv_file(file_path)
        to_be_scored__prospects_df = clean_prospect_datasets(prospect_df, file_path, key)
        umg_prospects.append(to_be_scored__prospects_df)

    for file_path in prospect_files:
        key = 'PATH_PROSPECTS_PD_' + file_path.split('/')[-1].split('.')[0].upper()
        prospect_df = rf.read_csv_file(file_path)
        cleaned_prospect_df = clean_prospect_datasets(prospect_df, file_path, key)
        umg_prospects.append(cleaned_prospect_df)

    # concatenate all datasets in a unified dataframe
    umg_prospects_df = pd.concat(umg_prospects)

    # Keeps in a csv format all leads that needs to be scored and already exist to our datasets
    keep_record_of_new_leads_that_already_exist_to_previous_datasets(umg_prospects_df)

    # keeps only unique rows. If we have a duplicate we keep drop the row where (umg_prospects_df['New_lead'] == ''
    # and we keep the row that has umg_prospects_df['New_lead'] == 'to_be_Scored'
    umg_prospects_df = drop_all_new_leads_that_already_exist_to_our_df(umg_prospects_df)

    final_umg_prospects_df = create_gender_column(umg_prospects_df)
    final_umg_prospects_df = final_umg_prospects_df.drop(['MI', 'SUFFIX'], axis=1)
    initial_prospects_df = reshape_prospects_df_into_pdr(final_umg_prospects_df)
    rf.store_df_in_pickle_format(ConfigUtils.conf, initial_prospects_df=initial_prospects_df)

import pandas as pd
import numpy as np
import reusable_functions as rf
from logger import Logger
from Aggregate_data.config.__init__ import Config
from Aggregate_data.config.config_utils import ConfigUtils


def create_temp_target_feature(df: pd.DataFrame) -> pd.DataFrame:
    # create a list of conditions to check for each row
    conditions = [
        # check if 'Status' column is not NaN
        df['Customer_contacted_status'].isin(['contacted']),
        # check if 'Calls_number' column is not NaN
        df['Calls_number'].notna(),
        # check if 'purl_fully_completed' column has 'Yes' or 'No' as values
        df['purl_fully_completed'].isin(['Yes', 'No'])
    ]

    # create a list of corresponding values to assign to 'Temporary_target' column based on the conditions
    values = ['contacted', 'contacted', 'contacted']

    # use np.select to assign values to 'Temporary_target' based on the conditions
    df['Temporary_target'] = np.select(conditions, values, default='uncontacted')

    return df


def add_reference_id_feature_to_purl_responders(ref_id_df: pd.DataFrame, responders_df: pd.DataFrame) -> pd.DataFrame:
    responders_df.rename(columns={'Reference ID': 'DM Reference ID'}, inplace=True)
    responders_df['DM Reference ID'] = responders_df['DM Reference ID'].apply(lambda x: rf.keep_number(x))
    ref_id_df['DM Reference ID'] = ref_id_df['DM Reference ID'].astype(str)
    # merge purl responders df with the Reference ID
    merged_df = pd.merge(ref_id_df, responders_df, on='DM Reference ID', how='right')
    # keep only the columns we are interested in
    merged_df = merged_df.loc[:, ['UUID', 'purl_fully_completed', 'f.Email', 'f.Phone']]
    # Drop all rows that hold Nan values in UUID column that came up from the above merge
    merged_df = merged_df.dropna(subset=['UUID'])
    return merged_df


def change_columns_order(df1: pd.DataFrame) -> pd.DataFrame:
    # create a list with the new order of column indices
    new_order = [0, 2, 35, 1, 33, 34, 11, 12, 13, 3, 4, 5, 6, 7, 8, 9, 10, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                 25, 26, 27, 28, 29, 30, 31, 32]
    # use the reindex method to change the column order
    return df1.reindex(columns=df1.columns[new_order])


def modify_input_phone_center_activity_values(df1: pd.DataFrame) -> pd.DataFrame:
    df1['input_phone_center_activity'] = df1['input_phone_center_activity'].fillna('').str. \
        replace('Sales_Inbound.*', 'Inbound', regex=True).str. \
        replace('Sales_Outbound.*', 'Outbound', regex=True).fillna('No contact')
    return df1


def migrate_and_discard_df_columns(df1: pd.DataFrame) -> pd.DataFrame:
    new_df1 = df1.drop(
        columns=['Status', 'Description', 'Customer_intention', 'Date Added', 'Lead Source',
                 'Direct Mail DID', 'Date', 'Trunk', 'Exit Reason'], axis=1)

    temp_df = new_df1.rename(
        columns={'UUID': 'input_feature_pd_customer_uuid', 'Customer_contacted_status': 'target',
                 'New_lead': 'input_feature_pd_customer_lead_to_be_scored',
                 'gender': 'input_feature_pd_customer_gender',
                 'Mail_number': 'input_feature_pd_mailings_sent_so_far',
                 'Calls_number': 'input_feature_pd_number_of_calls_so_far',
                 'First Name': 'input_feature_pd_first_name',
                 'Last Name': 'input_feature_pd_last_name',
                 'Lead purchased': 'input_feature_pd_date_of_lead_purchased',
                 'Zip_1': 'input_feature_pd_customer_zip1', 'Zip_2': 'input_feature_pd_customer_zip2',
                 'State': 'input_feature_pd_customer_state',
                 'Debt Amount': 'input_feature_pd_customer_debt_amount',
                 'purl_fully_completed': 'input_feature_pd_customer_purl_fully_completed',
                 'Queue': 'input_phone_center_activity',
                 'Call Time': 'input_feature_pd_customer_duration_call',
                 'Mean_Income': 'input_feature_3rdParty_mean_income_per_zip',
                 'Number_of_people_in_housing_units':
                     'input_feature_3rdParty_number_of_people_in_housing_units_per_zip',
                 'People_Income_Below_Poverty_Level':
                     'input_feature_3rdParty_people_Income_Below_Poverty_Level_per_zip',
                 'Housing_Units': 'input_feature_3rdParty_Housing_Units_per_zip',
                 'Occupied_Housing_Units': 'input_feature_3rdParty_Occupied_Housing_Units_per_zip',
                 'Monthly_Housing_Costs': 'input_feature_3rdParty_Monthly_Housing_Costs_per_zip',
                 'Number_of_noninstitutionalized_civilians':
                     'input_feature_3rdParty_Number_of_noninstitutionalized_civilians_per_zip',
                 'Insured_Civilians': 'input_feature_3rdParty_Insured_Civilians_per_zip',
                 'Uninsured_Civilians': 'input_feature_3rdParty_Uninsured_Civilians_per_zip',
                 'Population_Over_16': 'input_feature_3rdParty_Population_Over_16_per_zip',
                 'Employment_Rate': 'input_feature_3rdParty_Employment_Rate_per_zip',
                 'Number_of_Returns': 'input_feature_3rdParty_Number_of_Returns_per_zip',
                 'Number_of_individuals': 'input_feature_3rdParty_Number_of_individuals_per_zip',
                 'Total_Taxes_Paid_Amount': 'input_feature_3rdParty_Total_Taxes_Paid_Amount_per_zip'})

    return temp_df


def make_final_modifications(conf: Config, logger: Logger):
    add_us_census_bureau_df = rf.read_df_from_pickle_format(conf, 'add_us_census_bureau_df')
    purl_responders = rf.read_df_from_pickle_format(conf, 'purl_responders')
    lead_reference_lookup = rf.read_df_from_pickle_format(conf, 'lead_reference_lookup')

    purl_responders_with_reference_id = add_reference_id_feature_to_purl_responders(lead_reference_lookup,
                                                                                    purl_responders)
    purl_responders_with_reference_id['UUID'] = purl_responders_with_reference_id['UUID'].astype(str)
    add_us_census_bureau_df['UUID'] = add_us_census_bureau_df['UUID'].astype(str)
    # count the number of common UUID values in the two dataframes
    common_uuid_count = len(
        set(add_us_census_bureau_df['UUID']).intersection(set(purl_responders_with_reference_id['UUID'])))
    print(f"The two dataframes have {common_uuid_count} common UUID values.")
    final_df = pd.merge(add_us_census_bureau_df, purl_responders_with_reference_id, on='UUID', how='left')
    final_df = final_df.copy()
    temporary_target_df = create_temp_target_feature(final_df)
    new_df = migrate_and_discard_df_columns(temporary_target_df)
    modified_df = modify_input_phone_center_activity_values(new_df)
    # replace NaN values in the purl_fully_completed column with "Not clicked"
    modified_df['input_feature_pd_customer_purl_fully_completed'] = modified_df[
        'input_feature_pd_customer_purl_fully_completed'].fillna('Not Clicked')
    final_df_stage_one = change_columns_order(modified_df)
    rf.store_df_in_pickle_format(ConfigUtils.conf, final_df_stage_one=final_df_stage_one)

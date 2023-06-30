import pandas as pd
import reusable_functions as rf
import handle_debt_in_america as hdia
from Aggregate_data.config.config_utils import ConfigUtils
from logger import Logger
from typing import Tuple


def merge_external_data(census_2010: pd.DataFrame, census_acs_2017_2021: pd.DataFrame,
                        census_zip_codes_db_deluxe_business: pd.DataFrame,
                        census_zip_codes_place_fips: pd.DataFrame) -> pd.DataFrame:
    merged_df = pd.merge(census_2010, census_acs_2017_2021, on='ZIPCODE', how='left')
    merged_df = pd.merge(merged_df, census_zip_codes_db_deluxe_business, on='ZIPCODE', how='left')
    merged_df = pd.merge(merged_df, census_zip_codes_place_fips, on='ZIPCODE', how='left')
    # remove same columns with same name and contents
    merged_df = hdia.remove_suffixes_caused_by_merging_same_columns(merged_df)
    merged_df.rename(columns={"ZIPCODE": "input_feature_pd_customer_zip1"}, inplace=True)
    return merged_df


def ingest_census_zip_codes_place_fips() -> pd.DataFrame:
    with open('../Datasets/3rd_Party_Datasets/zip-codes-database-DELUXE-BUSINESS-csv/zip-codes-database-PLACE-FIPS.csv',
              'r', encoding='ISO-8859-1') as f:
        census_place_fips = pd.read_csv(f, encoding='utf-8')

    census_place_fips = rf.remove_all_columns_of_object_type(census_place_fips)

    # Group by ZipCode and calculate mean for each group
    census_place_fips_means = census_place_fips.groupby('ZIPCODE').mean().reset_index()

    census_place_fips_means = census_place_fips_means.dropna(subset=['ZIPCODE'])

    census_place_fips_means = census_place_fips_means.reset_index(drop=True)
    census_place_fips_means['ZIPCODE'] = census_place_fips_means['ZIPCODE'].astype(int).astype(str).str.zfill(5)
    return census_place_fips_means


def ingest_census_zip_codes_db_deluxe_business() -> pd.DataFrame:
    census_deluxe = rf.read_csv_file(ConfigUtils.conf.path_to_store_census_deluxe_df)

    census_deluxe = rf.remove_all_columns_of_object_type(census_deluxe)

    # Group by ZipCode and calculate mean for each group
    census_deluxe_means = census_deluxe.groupby('ZipCode').mean().reset_index()

    census_deluxe_means['ZipCode'] = census_deluxe_means['ZipCode'].apply(lambda x: str(x).zfill(5))

    census_deluxe_means.rename(columns={"ZipCode": "ZIPCODE"}, inplace=True)
    return census_deluxe_means


def ingest_census_2017_2021() -> pd.DataFrame:
    # The usecols parameter is used to specify a range of columns to be read from the CSV file. The range(524) specifies
    # that only the first 524 columns should be read.
    census_2017_2021 = rf.read_csv_file(ConfigUtils.conf.path_to_store_census_2017_2021_df,
                                        header=1, usecols=range(524))

    census_2017_2021['ZIPCODE'] = census_2017_2021['ZIPCODE'].apply(lambda x: str(x).zfill(5))

    return census_2017_2021


def ingest_census_2010() -> pd.DataFrame:
    census_df_2010 = rf.read_csv_file(ConfigUtils.conf.path_to_store_census_2010_df)

    census_df_2010['ZIPCode'] = census_df_2010['ZIPCode'].apply(lambda x: str(x).zfill(5))
    census_df_2010.rename(columns={"ZIPCode": "ZIPCODE"}, inplace=True)

    return census_df_2010


def format_census_deluxe_business_dataset(logger: Logger) -> Tuple[pd.DataFrame, str]:
    census_2010 = ingest_census_2010()
    census_acs_2017_2021 = ingest_census_2017_2021()
    census_zip_codes_db_deluxe_business = ingest_census_zip_codes_db_deluxe_business()
    census_zip_codes_place_fips = ingest_census_zip_codes_place_fips()

    merge_external_census_data = merge_external_data(census_2010, census_acs_2017_2021,
                                                     census_zip_codes_db_deluxe_business, census_zip_codes_place_fips)

    return merge_external_census_data, 'input_feature_pd_customer_zip1'

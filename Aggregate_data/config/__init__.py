from pydantic import BaseSettings, Field


class Config(BaseSettings):
    debug_mode: str = Field(..., env='DEBUG_MODE')
    pdr_files_path: str = Field(..., env='PDR_FILES_PATH')
    prospect_fullname_path: str = Field(..., env='PROSPECT_FULLNAME_PATH')
    prospects_to_be_scored_path: str = Field(..., env='PROSPECTS_TO_BE_SCORED_PATH')
    prospect_path: str = Field(..., env='PROSPECT_PATH')
    call_center_path: str = Field(..., env='CALL_CENTER_PATH')
    call_center_inbound_path: str = Field(..., env='CALL_CENTER_INBOUND_FORMAT_PATH')
    path_final_call_center: str = Field(..., env='PATH_FINAL_CALL_CENTER')
    path_opt_out_list: str = Field(..., env='PATH_OPT_OUT_LIST')
    path_to_leads_with_id_that_need_to_be_excluded: str = \
        Field(..., env='PATH_TO_LEADS_WITH_ID_THAT_NEED_TO_BE_EXCLUDED')
    path_to_leads_without_id_that_need_to_be_excluded: str = \
        Field(..., env='PATH_TO_LEADS_WITHOUT_ID_THAT_NEED_TO_BE_EXCLUDED')
    purl_responders_1022_0123_path: str = Field(..., env='PURL_RESPONDERS_1022_0123_PATH')
    path_purl: str = Field(..., env='PATH_PURL')
    path_phone: str = Field(..., env='PATH_PHONE')
    path_phone_20230308: str = Field(..., env='PATH_PHONE_20230308')
    path_status: str = Field(..., env='PATH_STATUS')
    path_status_description: str = Field(..., env='PATH_STATUS_DESCRIPTION')
    path_final_dataset: str = Field(..., env='PATH_FINAL_DATASET')
    path_to_us_census_bureau_3rd_party_data: str = Field(..., env='PATH_TO_US_CENSUS_BUREAU_3RD_PARTY_DATA')
    path_debt_in_america_june_2022_auto: str = Field(..., env='PATH_DEBT_IN_AMERICA_JUNE_2022_AUTO')
    path_debt_in_america_june_2022_delinquency: str = Field(..., env='PATH_DEBT_IN_AMERICA_JUNE_2022_DELINQUENCY')
    path_debt_in_america_june_2022_medical: str = Field(..., env='PATH_DEBT_IN_AMERICA_JUNE_2022_MEDICAL')
    path_debt_in_america_june_2022_student: str = Field(..., env='PATH_DEBT_IN_AMERICA_JUNE_2022_STUDENT')
    path_to_df_matching_zipcodes_to_geoids: str = Field(..., env='PATH_TO_DF_MATCHING_ZIPCODES_TO_GEOIDS')
    path_federal_reserve_bank_philadelphia: str = Field(..., env='PATH_FEDERAL_RESERVE_BANK_PHILADELPHIA')
    path_to_store_preprocessed_bank_of_philadelphia_files: str = \
        Field(..., env='PATH_TO_STORE_PREPROCESSED_BANK_OF_PHILADELPHIA_FILES')
    path_to_read_all_preprocessed_bank_of_philadelphia_files: str = \
        Field(..., env='PATH_TO_READ_ALL_PREPROCESSED_BANK_OF_PHILADELPHIA_FILES')
    path_to_store_bank_of_philadelphia_final_file: str = \
        Field(..., env='PATH_TO_STORE_BANK_OF_PHILADELPHIA_FINAL_FILE')
    path_to_store_census_2010_df: str = Field(..., env='PATH_TO_STORE_CENSUS_2010_DF')
    path_to_store_census_2017_2021_df: str = Field(..., env='PATH_TO_STORE_CENSUS_2017_2021_DF')
    path_to_store_census_deluxe_df: str = Field(..., env='PATH_TO_STORE_CENSUS_DELUXE_DF')
    path_to_store_pickle_files: str = Field(..., env='PATH_TO_STORE_PICKLE_FILES')

    class Config:
        case_sensitive = False
        env_file = '.env'
        env_file_encoding = 'utf-8'

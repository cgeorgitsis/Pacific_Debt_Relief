import os
import pandas as pd
import reusable_functions as rf
import pdr_file_preprocessing as pfp
import status_preprocessing as sp
import unify_and_clean_call_center_datasets as ucd
import phone_and_trunk_preprocessing as php
import handle_zip_code as hzc
import external_dataset_added as eda
import rename_drop_columns as rdc
import unify_and_clear_prospect_datasets as ucpd
import unify_and_clear_purl_responder_datasets as ucord
import unify_prospects_pdr_dataframes as upd
import clear_opt_out_list_and_remove_opt_out_prospects as colarop
import census_deluxe_business as cdb
import handle_debt_in_america as hdia
import federal_reserve_bank_philadelphia as frbp
from logger import Logger
from config import Config

if __name__ == "__main__":
    conf = Config(debug_mode=True)

    log = Logger("PACIFIC DEBT")
    logger = log.getLogger()
    logger.info("Start the procedure in order to contain the final dataset")

    colarop.clear_opt_out_list()
    ucpd.format_umg_datasets()
    colarop.remove_prospects_without_reference_id(conf, logger)
    pfp.format_pdr_file_dataset()
    colarop.remove_prospects_with_reference_id(conf, logger)
    upd.helper_concatenation_function(conf, logger)
    sp.format_status_dataset(conf, logger)
    ucd.format_call_center_dataset(logger)
    php.format_phone_trunk_dataset(conf, logger)
    hzc.format_description_dataset(conf, logger)
    ucord.format_purl_responders(logger)
    eda.format_external_dataset(conf, logger)
    rdc.make_final_modifications(conf, logger)
    final_df = rf.read_df_from_pickle_format(conf, 'final_df_stage_one')
    final_df['input_feature_pd_customer_zip1'] = final_df['input_feature_pd_customer_zip1'].astype(str)

    cases_df = None
    onParam = None
    if os.getenv('NUMBER_OF_FEATURES') == '1':
        cases_df, onParam = hdia.format_debt_in_america_datasets(logger)
    elif os.getenv('NUMBER_OF_FEATURES') == '2':
        cases_df, onParam = cdb.format_census_deluxe_business_dataset(logger)
    elif os.getenv('NUMBER_OF_FEATURES') == '3':
        cases_df, onParam = frbp.format_federal_reserve_bank_philadelphia_dataset(logger)
    elif os.getenv('NUMBER_OF_FEATURES') == '4':
        cases_df, onParam = hdia.format_debt_in_america_datasets(logger)
        final_df = pd.merge(final_df, cases_df, on=onParam, how='left', sort=False)
        cases_df, onParam = frbp.format_federal_reserve_bank_philadelphia_dataset(logger)

    if os.getenv('NUMBER_OF_FEATURES') in ['1', '2', '3', '4']:
        final_df = pd.merge(final_df, cases_df, on=onParam, how='left', sort=False)

    final_df.to_csv(os.environ.get('PATH_FINAL_DATASET'), sep=',', encoding='utf-8', index=False)

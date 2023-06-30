import glob, os
import pandas as pd
import numpy as np
import gender_guesser.detector as gender
import reusable_functions as rf
from Aggregate_data.config.config_utils import ConfigUtils
from logger import Logger


def check_completed(row: pd.DataFrame) -> str:
    """
    This code checks if at least one of the columns 'f.First' or 'f.Last', or both, has a value. If at least one of
    these columns has a value, it means that the lead has clicked the PURL and fully completed the form. In this case,
    the new column 'purl_completed' is assigned the value 'Yes'. On the other hand, if both 'f.First' and 'f.Last'
    columns are empty or contain missing values, it means that the lead has clicked the PURL but did not fully complete
    the form. In this case, the 'purl_completed' column is assigned the value 'No'.
    :param row: Our dataframe containing all leads that clicked the PURL.
    :return: 'Yes' or 'No'
    """
    if pd.isna(row['f.First']) or pd.isna(row['f.Last']):
        return 'No'
    else:
        return 'Yes'


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
    df.loc[:, 'gender'] = df['First'].apply(
        lambda x: map_gender(detector.get_gender(x)) if isinstance(x, str) else np.nan)
    return df.copy()


def format_purl_responders(logger: Logger):
    umg_prospects = []

    for path in glob.glob(ConfigUtils.conf.purl_responders_1022_0123_path):
        filename, extension = os.path.splitext(path)
        if extension == '.csv':
            fullname_prospect_df = pd.read_csv(path)
        elif extension == '.xlsx':
            fullname_prospect_df = pd.read_excel(path)
        else:
            raise ValueError(f"Unsupported file type: {path}")

        umg_prospects.append(fullname_prospect_df)

    # concatenate all datasets in a unified dataframe
    purl_responders_df = pd.concat(umg_prospects)

    purl_responders_df.drop_duplicates(subset=['Reference ID', 'Zip', 'Debt Amount'], keep='first', inplace=True)
    final_purl_responders_df = create_gender_column(purl_responders_df)

    # Contains all leads that fully completed the form
    final_purl_responders_df['purl_fully_completed'] = final_purl_responders_df.apply(check_completed, axis=1)
    rf.store_df_in_pickle_format(ConfigUtils.conf, purl_responders=final_purl_responders_df)

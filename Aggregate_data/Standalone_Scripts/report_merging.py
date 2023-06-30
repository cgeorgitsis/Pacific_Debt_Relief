import pandas as pd

single_excel_file = pd.read_excel("../../Datasets/Raw_Data/Report 2568404 - Test - Full Distribution Report.xlsx"
                                  , sheet_name=None)
# sheet names
print(single_excel_file.keys())

exclude_column_names = ['Cover Sheet', 'Trunk Distribution', 'Call Times', 'Call Distribution', 'Call Times 1',
                        'Call Distribution 1', 'Call Times 2', 'Call Distribution 2', 'Call Times 3']
keep_column_names = single_excel_file.keys() - exclude_column_names

dict_you_want = {key: single_excel_file[key] for key in keep_column_names}
dataset_combined = pd.concat(dict_you_want.values())
dataset_combined.drop('#', axis=1, inplace=True)
dataset_combined.to_csv('../../Datasets/concatenated_call_center_data.csv', sep=',',
                        encoding='utf-8', index=False)

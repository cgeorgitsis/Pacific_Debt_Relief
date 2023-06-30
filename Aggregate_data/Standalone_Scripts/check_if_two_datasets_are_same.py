import pandas as pd

sabino_dataset = pd.read_csv("../Datasets/SabinoDB.txt", sep=',')
print(sabino_dataset['Lead Source'].nunique())

sabino_ingestion = pd.read_excel("../Datasets/SabinoDB-DMIngestion-Report.xlsx",
                                 sheet_name='SabinoDB-DMIngestion-Report')


dfRes = pd.concat([sabino_dataset, sabino_ingestion])

# reset index
dfRes = dfRes.reset_index(drop=True)

# group by columns
dfGroup = dfRes.groupby(list(dfRes.columns))

# length of each row to calculate the count
# if count is greater than 1, that would mean common rows
res = [k[0] for k in dfGroup.groups.values() if len(k) > 1]

print(len(dfRes.reindex(res)))
import pandas as pd 

dict_data = {"a": [10, 20, 30, 40], "b": [50, 60, 70, 80]}

df_by_columns = pd.DataFrame.from_dict(dict_data, orient="columns")
print("dataframe created from from_dict")
print(df_by_columns)

cols = ['number_1', 'number_2', 'number_3', 'number_4']
df_by_index = pd.DataFrame.from_dict(dict_data, orient="index", columns=cols)
print("dataframe created from from_dict and set the orient")
print(df_by_index)
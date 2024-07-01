import pandas as pd

pd.set_option('display.max_columns', None)

# example: read the first row, use original column names
df = pd.read_csv("../dataset/sample.csv", sep=",")
print("Print the first row")
print(df.head(1))
print("--------------------")

# indexing: selecting single column
df_single_col = df["passenger_count"]
print("Selecting single column")
print(df_single_col)
print("--------------------")

# indexing: selecting multiple columns
df_multiple_cols = df[["VendorID", "passenger_count", "trip_distance"]]
print("Selecting multiple columns")
print(df_multiple_cols)
print("--------------------")

# indexing: selecting a single row

print("list of index: ", df.index)
df_single_row = df.loc[0]
print("Selecting a single row, index 0")
print(df_single_row)
print("--------------------")

# indexing: selecting multiple rows
df_multiple_rows = df.loc[:5] # equal to df.loc[[0,1,2,3,4,5]]
print("Selecting multiple rows, index 0-5")
print(df_multiple_rows)
print("--------------------")

# indexing: selecting multiple rows and columns
df_multiple_rows_cols = df.loc[:5, ["VendorID", "passenger_count", "trip_distance"]]
print("Selecting multiple rows and cols")
print(df_multiple_rows_cols)
print("--------------------")

# example: read only 10 rows
df = pd.read_csv("../dataset/sample.csv", sep=",", nrows=10)
print("only read 10 rows")
print("The number of row in this dataframe is {}".format(df.shape[0]))
print("--------------------")

# example: read only first row and replace column names
df = pd.read_csv("../dataset/sample.csv", sep=",", header=0, names=["new_column_aa", "new_column_bb", "new_column_cc"])
print("use the new column names")
print(df.head(1))
print("--------------------")

# example: read from selected column only
df = pd.read_csv("../dataset/sample.csv", sep=",", header=0, usecols=["tpep_dropoff_datetime"])
print("only rad the tpep_dropoff_datetime column")
print(df.head())

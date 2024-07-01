import pandas as pd

df  = pd.read_parquet('../dataset/yellow_tripdata_2023-01.parquet', engine="pyarrow")

print(df.dtypes)
import pandas as pd

df  = pd.read_json('../dataset/2017-10-02-1.json', lines=True, orient='records')

print(df)
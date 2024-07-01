import pandas as pd

year, month, day, hour = 2017, 10, 2, 1
url = f"http://data.gharchive.org/{year}-{month:02}-{day:02}-{hour}.json.gz"

# 'User-Agent' header to ensure that API requests are identified as coming from Pandas.
storage_options = {'User-Agent': 'pandas'}

df = pd.DataFrame()

with pd.read_json(url, lines=True, storage_options=storage_options, chunksize=50000, compression="gzip") as reader:
    for chunk in reader:
        df = pd.concat([df, chunk], ignore_index=True)
        print(df)

print(df)

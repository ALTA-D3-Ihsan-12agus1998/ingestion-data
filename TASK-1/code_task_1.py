import pandas as pd

def process_data(file_path):
    pd.set_option('display.max_columns', 18)
    df = pd.read_csv(file_path, sep=",", header=0, names=[
        "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "ratecode_id", "store_and_fwd_flag", "pulocation_id", "dolocation_id",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge"
    ])

    # Rename columns to snake_case [Task No.2]
    df.columns = [col.lower() for col in df.columns]

    selected_cols = ["vendor_id", "passenger_count", "trip_distance", 
                     "payment_type", "fare_amount", "extra", "mta_tax", 
                     "tip_amount", "tolls_amount", "improvement_surcharge", 
                     "total_amount", "congestion_surcharge"]
    df_sorted = df[selected_cols].sort_values(by='passenger_count', ascending=False)

    # Display top 10 rows with highest passenger_count [Task No.3]
    print(df_sorted.head(10))

# Take the yellow_tripdata_2020-07csv file from the dataset [Task No.1]
process_data("../dataset/yellow_tripdata_2020-07.csv")

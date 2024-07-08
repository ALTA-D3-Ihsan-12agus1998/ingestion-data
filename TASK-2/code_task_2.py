import pandas as pd
from sqlalchemy import create_engine, BigInteger, String, JSON, DateTime, Boolean
from sqlalchemy.exc import SQLAlchemyError

class Extraction:
    def __init__(self):
        self.dataframe = pd.DataFrame()

    def local_file(self, path: str):
        self.extension = path.split(".")[-1]
        read_funcs = {
            "csv": self._read_csv,
            "json": self._read_json,
            "parquet": self._read_parquet
        }
        read_func = read_funcs.get(self.extension)
        if read_func:
            read_func(path)
        self._investigate_schema()
        self._cast_data()
        return self.dataframe

    def _read_json(self, path):
        self.dataframe = pd.read_json(path, lines=True)

    def _read_parquet(self, path):
        self.dataframe = pd.read_parquet(path, engine="pyarrow")

    def _read_csv(self, path):
        self.dataframe = pd.read_csv(path)

    def request_api(self, url: str):
        self.dataframe = pd.read_json(url, lines=True, chunksize=50000, storage_options={'User-Agent': 'pandas'})
        self._investigate_schema()
        self._cast_data()
        return self.dataframe

    def _investigate_schema(self):
        pd.set_option('display.max_columns', None)
        print("DataFrame head:\n", self.dataframe.head())
        print("DataFrame info:\n", self.dataframe.info())

    def _cast_data(self):
        if self.extension == "json":
            self.dataframe["id"] = self.dataframe["id"].astype("Int64")
            self.dataframe["type"] = self.dataframe["type"].astype("string")
            self.dataframe["public"] = self.dataframe["public"].astype("string")
            self.dataframe["created_at"] = pd.to_datetime(self.dataframe["created_at"])

class Load:
    def __init__(self):
        self.engine = None

    def _create_connection(self):
        user = "postgres"
        password = "admin"
        host = "localhost"
        database = "mydb"
        port = 5432
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(conn_string)

    def to_postgres(self, db_name: str, data: pd.DataFrame):
        self._create_connection()
        df_schema = {
            "id": BigInteger,
            "type": String(100),
            "actor": JSON,
            "repo": JSON,
            "payload": JSON,
            "public": Boolean,
            "created_at": DateTime,
            "org": JSON
        }
        try:
            data.to_sql(name=db_name, con=self.engine, if_exists="replace", index=False, dtype=df_schema, chunksize=5000)
        except SQLAlchemyError as err:
            print("Error:", err)

def main():
    extract = Extraction()
    file_path = "../dataset/yellow_tripdata_2023-01.parquet"
    df_result = extract.local_file(file_path)
    load = Load()
    db_name = "yellow_trip_parquet"
    load.to_postgres(db_name, df_result)

if __name__ == "__main__":
    main()

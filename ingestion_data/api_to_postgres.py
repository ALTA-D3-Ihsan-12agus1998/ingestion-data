import pandas as pd

class Extraction():
    def __init__(self) -> None:
        self.path: str
        self.url: str
        self.dataframe = pd.DataFrame()


    def request_api(self, url, save_path) -> pd.DataFrame:
        self.url = url
        self.__read_json_chunked()
        self.investigate_schema()
        self.cast_data()

        if save_path:
            self.dataframe.to_json(save_path, orient='records', lines=True)
    
        return self.dataframe


    def __read_json_chunked(self) -> None:
        """Read github data from web with read_json to pandas DataFrame"""
        storage_options = {'User-Agent': 'pandas'}

        """

        storage_options (optional)
        Extra options that make sense for a particular storage connection, 
        e.g. host, port, username, password, etc. 
        
        For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options.

        """

        chunk_size = 50000
        with pd.read_json(self.url, lines=True, storage_options=storage_options, chunksize=chunk_size, compression="gzip") as reader: 
            for chunk in reader:
                self.dataframe = pd.concat([self.dataframe, chunk], ignore_index=True)


    def investigate_schema(self):
        pd.set_option('display.max_columns', None )

        org_nan_value = self.dataframe["org"].isnull().sum()
        print("org_nan_value \n", org_nan_value)

        type_nan_value = self.dataframe["type"].isnull().sum()
        print("type_nan_value \n", type_nan_value)
       

    def cast_data(self):
        self.dataframe["id"] = self.dataframe["id"].astype("Int64")
        self.dataframe["type"] = self.dataframe["type"].astype("string")
        self.dataframe["public"] = self.dataframe["public"].astype("boolean")
        self.dataframe["created_at"] = pd.to_datetime(self.dataframe["created_at"])
      
    
class Load():
    # https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
    def __init__(self) -> None:
        self.df = pd.DataFrame
        self.db_name = ""
        self.engine = None
        self.connection = None
    
    def __create_connection(self):
        from sqlalchemy import create_engine 

        user = "postgres"
        password = "admin"
        host = "localhost"
        database = "mydb"
        port = 5432
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        self.engine = create_engine(conn_string) 

    def to_postgres(self, db_name: str, data: pd.DataFrame):
        from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean
        from sqlalchemy.exc import SQLAlchemyError

        self.db_name = db_name
        self.__create_connection()
        try:
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

            data.to_sql(name=self.db_name, con=self.engine, if_exists="replace", index=False, schema="public", dtype=df_schema, method=None, chunksize=5000)
        except SQLAlchemyError as err:
            print("error >> ", err.__cause__)
          

def main():
    extract = Extraction()
    # year, month, day, hour = 2023, 10, 1, 1
    year, month, day, hour = 2017, 10, 2, 1
    url = f"http://data.gharchive.org/{year}-{month:02}-{day:02}-{hour}.json.gz"
    save_path = f'../dataset/{year}-{month:02}-{day:02}-{hour}.json'
    print("url: ", url)
    df_result = extract.request_api(url, save_path)

    # load = Load()
    # db_name = "github_data"
    # load.to_postgres(db_name, df_result)


if __name__ == "__main__":
    main()

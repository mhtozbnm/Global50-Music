# data_loading.py


import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from config import config

from sqlalchemy import create_engine

class DataLoading:
    def __init__(self):
        self.engine = self.create_db_engine()

    def create_db_engine(self):
        db_config = config.DATABASE
        connection_string = f"{db_config['drivername']}://" \
                            f"{db_config['username']}:{db_config['password']}@" \
                            f"{db_config['host']}:{db_config['port']}/" \
                            f"{db_config['database']}"
        engine = create_engine(connection_string)
        return engine
    
    def upload_DB(self, df, table_name='spotify_tracks'):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Veriler başarıyla '{table_name}' tablosuna yüklendi.")
        except Exception as e:
            print(f"Veri yükleme sırasında bir hata oluştu: {e}")

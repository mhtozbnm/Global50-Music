# data_loading.py


import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from config import config

from sqlalchemy import create_engine
import logging

class DataLoading:
    def __init__(self):
        self.engine = self.create_db_engine()

    def create_db_engine(self):
        try:
            db_config = config.DATABASE
            connection_string = f"{db_config['drivername']}://" \
                                f"{db_config['username']}:{db_config['password']}@" \
                                f"{db_config['host']}:{db_config['port']}/" \
                                f"{db_config['database']}"
            engine = create_engine(connection_string)
            logging.info('Veritabanı bağlantısı başarılı.')
            return engine
        except Exception as e:
            logging.error(f'Veritabanı bağlantısı başarısız: {e}')
            raise

    def upload_DB(self, df, table_name):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            logging.info(f'Veriler {table_name} tablosuna yüklendi.')
        except Exception as e:
            logging.error(f'Veri yükleme sırasında hata oluştu: {e}')
            raise

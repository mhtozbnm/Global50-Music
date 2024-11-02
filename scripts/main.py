# main.py

from data_extraction import DataExtraction
from data_transformation import DataTransformation
from data_loading import DataLoading
import logging
import os
import datetime

def main():

    # Loglama ayarları
    logging.basicConfig(filename='logs/automation.log', level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')

    try:
        logging.info('Otomasyon başlatıldı.')

        # Veri Çekme
        data_extractor = DataExtraction()
        df = data_extractor.get_data()

        today = datetime.date.today().strftime('%Y%m%d')
        output_folder_raw = 'data/raw'
        if not os.path.exists(output_folder_raw):
            os.makedirs(output_folder_raw)
        csv_file_path = os.path.join(output_folder_raw, f'spotify_data_{today}.csv')
        df.to_csv(csv_file_path, index=False)
        logging.info(f'Ham veriler CSV olarak kaydedildi: {csv_file_path}')

        data_transformation = DataTransformation()
        final_df = data_transformation.edited_data(df)

        output_folder_processed = 'data/processed'
        if not os.path.exists(output_folder_processed):
            os.makedirs(output_folder_processed)
        csv_file_path = os.path.join(output_folder_processed, f'spotify_data_{today}.csv')
        df.to_csv(csv_file_path, index=False)
        logging.info(f'Veriler CSV olarak kaydedildi: {csv_file_path}')
        
        # Veri Yükleme
        data_loader = DataLoading()
        data_loader.upload_DB(final_df,'spotify_top_50')
        logging.info('Veriler veritabanına yüklendi.')

        logging.info('Otomasyon başarıyla tamamlandı.')

    except Exception as e:
        logging.error(f'Otomasyon sırasında hata oluştu: {e}')
    

if __name__ == '__main__':
    main()

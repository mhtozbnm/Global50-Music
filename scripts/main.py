# main.py

from data_extraction import DataExtraction
from data_transformation import DataTransformation
from data_loading import DataLoading
import logging
import os
import datetime

def main():
    # Log klasörünü oluştur
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Loglama ayarları güncellendi
    logging.basicConfig(
        filename='logs/automation.log',
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Ayrıca console'a da log basalım
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger('').addHandler(console_handler)

    try:
        logging.info('Otomasyon başlatıldı.')

        # Veri Çekme
        data_extractor = DataExtraction()
        df = data_extractor.get_data()

        if df is None or df.empty:
            logging.error('Veri çekilemedi veya boş DataFrame döndü.')
            return

        # Klasörleri oluştur
        output_folder_raw = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
        output_folder_processed = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

        for folder in [output_folder_raw, output_folder_processed]:
            if not os.path.exists(folder):
                os.makedirs(folder)
                logging.info(f'Klasör oluşturuldu: {folder}')

        # Ham veriyi kaydet
        today = datetime.date.today().strftime('%Y%m%d')
        raw_csv_path = os.path.join(output_folder_raw, f'spotify_data_{today}.csv')
        
        try:
            df.to_csv(raw_csv_path, index=False)
            logging.info(f'Ham veriler kaydedildi: {raw_csv_path}')
        except Exception as e:
            logging.error(f'Ham veri kaydedilirken hata: {e}')

        # Veri Dönüşümü
        data_transformation = DataTransformation()
        final_df = data_transformation.edited_data(df)

        if final_df is None or final_df.empty:
            logging.error('Veri dönüşümü başarısız oldu.')
            return

        # İşlenmiş veriyi kaydet
        processed_csv_path = os.path.join(output_folder_processed, f'spotify_data_{today}.csv')
        
        try:
            final_df.to_csv(processed_csv_path, index=False)
            logging.info(f'İşlenmiş veriler kaydedildi: {processed_csv_path}')
        except Exception as e:
            logging.error(f'İşlenmiş veri kaydedilirken hata: {e}')

        # Veri Yükleme
        try:
            data_loader = DataLoading()
            data_loader.upload_DB(final_df, 'spotify_top_50')
            logging.info('Veriler veritabanına yüklendi.')
        except Exception as e:
            logging.error(f'Veritabanına yükleme hatası: {e}')

        logging.info('Otomasyon başarıyla tamamlandı.')

    except Exception as e:
        logging.error(f'Otomasyon sırasında hata oluştu: {e}')
        raise
    

if __name__ == '__main__':
    main()

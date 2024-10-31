# data_loading.py

import os
from data_extraction import DataExtraction

class DataLoading:
    
    def edited_data(self, df):
        df.drop(['uri','track_href','track_href','duration_ms','track_id','analysis_url','type'],axis=1, inplace=True)
        return df
    
    def save_data(self, df):
        output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'processed'))
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        csv_file_path = os.path.join(output_folder, 'processed_spotify_data.csv')
        df.to_csv(csv_file_path, index=False)
        print(f"Veri başarıyla {csv_file_path} konumuna kaydedildi.")
        
    
# data_extraction.py

import sys
import os

# Proje ana dizinine giden yolu bulun ve sys.path'e ekleyin
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from config import config  # config klasöründen config.py modülünü import ediyoruz

class DataExtraction:
    def __init__(self):
        self.sp = self.connect_api()
    
    def connect_api(self):
        auth_manager = SpotifyClientCredentials(
            client_id=config.client_id,
            client_secret=config.client_secret)
        sp = spotipy.Spotify(auth_manager=auth_manager)
        return sp
    
    def get_data(self, playlist_id='37i9dQZEVXbMDoHDwVN2tF'):
        try:
            results = self.sp.playlist_items(playlist_id)
    
            tracks = results['items']
            track_list = []
    
            for item in tracks:
                track = item['track']
                track_info = {
                    'track_name': track.get('name'),
                    'artist_name': track['artists'][0]['name'] if track.get('artists') else None,
                    'album_name': track['album'].get('name') if track.get('album') else None,
                    'release_date': track['album'].get('release_date') if track.get('album') else None,
                    'total_tracks': track['album'].get('total_tracks') if track.get('album') else None,
                    'popularity': track.get('popularity'),
                    'track_id': track.get('id')
                }
                track_list.append(track_info)
    
            df = pd.DataFrame(track_list)
    
            # Ses özelliklerini ekleyelim
            audio_features = []
    
            for track_id in df['track_id']:
                features = self.sp.audio_features(track_id)[0]
                audio_features.append(features)
    
            features_df = pd.DataFrame(audio_features)
    
            # İki DataFrame'i birleştirelim
            final_df = pd.concat([df, features_df], axis=1)
    
            return final_df
        
        except Exception as e:
            print(f"Veri çekme işleminde hata oluştu: {e}")
            return None

# DataFrame'i CSV olarak kaydetme fonksiyonu ekleyelim
    def save_data(self, df):
        output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        csv_file_path = os.path.join(output_folder, 'spotify_data.csv')
        df.to_csv(csv_file_path, index=False)
        print(f"Veri başarıyla {csv_file_path} konumuna kaydedildi.")

# Eğer bu dosya doğrudan çalıştırılırsa
if __name__ == '__main__':
    data_extractor = DataExtraction()
    final_df = data_extractor.get_data()
    data_extractor.save_data(final_df)
    

    
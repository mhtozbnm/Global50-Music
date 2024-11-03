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
import logging

class DataExtraction:
    def __init__(self):
        self.sp = self.connect_api()
    
    def connect_api(self):
        try:
            auth_manager = SpotifyClientCredentials(
                client_id=config.client_id,
                client_secret=config.client_secret)
            sp = spotipy.Spotify(auth_manager=auth_manager)
            logging.info('Spotify API bağlantısı başarılı.')
            return sp
        except Exception as e:
            logging.error(f'Spotify API bağlantısı başarısız: {e}')
            raise


    def get_data(self, playlist_id='37i9dQZEVXbMDoHDwVN2tF'):
        try:
            # Playlist kontrolü
            try:
                results = self.sp.playlist_items(
                    playlist_id,
                )
                logging.info(f'Playlist başarıyla alındı: {playlist_id}')
            except spotipy.exceptions.SpotifyException as e:
                logging.error(f'Spotify API hatası: {e}')
                return None
            except Exception as e:
                logging.error(f'Playlist alınırken beklenmeyen hata: {e}')
                return None

            tracks = results['items']
            track_list = []

            # Her bir parça için bilgi toplama
            for item in tracks:
                try:
                    if not item or 'track' not in item or not item['track']:
                        continue

                    track = item['track']
                    
                    # Temel kontroller
                    if not track.get('id'):
                        continue

                    track_info = {
                        'track_name': track.get('name', 'Bilinmeyen'),
                        'artist_name': track.get('artists', [{}])[0].get('name', 'Bilinmeyen'),
                        'album_name': track.get('album', {}).get('name', 'Bilinmeyen'),
                        'release_date': track.get('album', {}).get('release_date', None),
                        'total_tracks': track.get('album', {}).get('total_tracks', 0),
                        'popularity': track.get('popularity', 0),
                        'track_id': track.get('id')
                    }
                    track_list.append(track_info)
                    logging.debug(f'Şarkı bilgisi eklendi: {track_info["track_name"]}')

                except Exception as e:
                    logging.warning(f'Şarkı bilgisi işlenirken hata: {e}')
                    continue

            if not track_list:
                logging.error('Hiç geçerli şarkı bulunamadı.')
                return None

            df = pd.DataFrame(track_list)
            logging.info(f'Toplam {len(df)} şarkı bilgisi alındı.')

            # Ses özelliklerini batch halinde alma
            audio_features = []
            batch_size = 50  # Spotify API sınırı

            # Track ID'leri None olmayanları filtrele
            valid_track_ids = df['track_id'].dropna().tolist()

            for i in range(0, len(valid_track_ids), batch_size):
                batch_ids = valid_track_ids[i:i+batch_size]
                try:
                    features_batch = self.sp.audio_features(batch_ids)
                    # None olmayan özellikleri ekle
                    valid_features = [f for f in features_batch if f is not None]
                    audio_features.extend(valid_features)
                    logging.debug(f'Batch {i//batch_size + 1} ses özellikleri alındı')
                except Exception as e:
                    logging.error(f'Ses özellikleri alınırken hata (batch {i//batch_size + 1}): {e}')
                    continue

            if not audio_features:
                logging.error('Hiç ses özelliği alınamadı.')
                return df  # En azından temel verileri döndür

            features_df = pd.DataFrame(audio_features)

            # DataFrame'leri birleştirme
            try:
                # track_id'ye göre birleştirme
                final_df = pd.merge(
                    df,
                    features_df,
                    left_on='track_id',
                    right_on='id',
                    how='left'
                )
                
                # Gereksiz sütunları temizle
                if 'id' in final_df.columns:
                    final_df = final_df.drop('id', axis=1)
                    
                logging.info(f'Veri başarıyla işlendi. Final DataFrame boyutu: {final_df.shape}')
                return final_df

            except Exception as e:
                logging.error(f'DataFrame birleştirme hatası: {e}')
                return df

        except Exception as e:
            logging.error(f'Genel veri çekme hatası: {e}')
            return None

# DataFrame'i CSV olarak kaydetme fonksiyonu ekleyelim
    def save_data(self, df):
        output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        csv_file_path = os.path.join(output_folder, 'raw_spotify_data.csv')
        df.to_csv(csv_file_path, index=False)
        print(f"Veri başarıyla {csv_file_path} konumuna kaydedildi.")


    

    
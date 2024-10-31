# main.py

from data_extraction import DataExtraction

def main():
    data_extractor = DataExtraction()
    df = data_extractor.get_data()
    print(df.head())  # Veri kontrolü için ilk birkaç satırı yazdırabilirsiniz

    # İsterseniz burada df'yi kaydedebilir veya sonraki işlemlere aktarabilirsiniz
    # Örneğin, df'yi CSV olarak kaydetmek:
    # df.to_csv('data/processed/spotify_data.csv', index=False)

    

if __name__ == '__main__':
    main()

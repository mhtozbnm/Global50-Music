# main.py

from data_extraction import DataExtraction

def main():
    data_extractor = DataExtraction()
    df = data_extractor.get_data()
    data_extractor.save_data(df)
    print(df.head())  # Veri kontrolü için ilk birkaç satırı yazdırabilirsiniz

    

if __name__ == '__main__':
    main()

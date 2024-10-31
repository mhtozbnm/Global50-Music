# main.py

from data_extraction import DataExtraction

def main():
    data_extractor = DataExtraction()
    df = data_extractor.get_data()
    print(df.head())  # Veri kontrolü için ilk birkaç satırı yazdırabilirsiniz

    

if __name__ == '__main__':
    main()

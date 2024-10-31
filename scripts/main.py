# main.py

from data_extraction import DataExtraction
from data_loading import DataLoading

def main():
    data_extractor = DataExtraction()
    data_loading = DataLoading()
    df = data_extractor.get_data()
    data_extractor.save_data(df)
    print(df.head())  # Veri kontrolü için ilk birkaç satırı yazdırabilirsiniz

    edited_df = data_loading.edited_data(df)
    data_loading.save_data(edited_df)
    

    

if __name__ == '__main__':
    main()

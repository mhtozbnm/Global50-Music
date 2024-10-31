# main.py

from data_extraction import DataExtraction
from data_transformation import DataTransformation
from data_loading import DataLoading

def main():
    data_extractor = DataExtraction()
    data_transformation = DataTransformation()
    data_loader = DataLoading()
    df = data_extractor.get_data()
    data_extractor.save_data(df)
    print(df.head())  # Veri kontrolü için ilk birkaç satırı yazdırabilirsiniz

    edited_df = data_transformation.edited_data(df)
    print(edited_df.head())
    data_transformation.save_data(edited_df)

    data_loader.upload_DB(edited_df)
    

    

if __name__ == '__main__':
    main()

import os
import pandas as pd
import json
import datetime

def load_category_mapping(json_path):
    """
    Read JSON files mapping with each country
    :param json_path: Path to JSON files.
    :return: A dict mapping category_id -> category_name
    """
    with open(json_path,'r',encoding='utf-8') as f:
        data = json.load(f)
        return {int(item['id']) : item['snippet']['title'] for item in data['items']}

def read_csv_with_fallback_encoding(path):
    """
    Check encoding for CSV files:
    :param: path to the files.
    :return: A checked encoding DataFrame.
    """
    encodings_to_try = ['utf-8','utf-8-sig','ISO-8859-1','cp932']
    for enc in encodings_to_try:
        try:
            return pd.read_csv(path,encoding=enc)
        except UnicodeDecodeError:
            print(f'Failed to read {path} with encoding: {enc}')
    raise ValueError(f'Cannot read file: {path} with know encodings')

def extract_youtube_data():
    raw_data_path = '/home/thangtranquoc/youtube-project/youtube-trending-etl/data/raw'
    output_path = '/home/thangtranquoc/youtube-project/youtube-trending-etl/data/processed/parquet'
    extension = '.csv'
    for file in os.listdir(raw_data_path):
        if file.endswith(extension):
            # Lấy mã quốc theo theo 2 chữ cái đầu tiên của video
            country_code = file[:2]
            # Path to JSON and CSV files.
            csv_path = os.path.join(raw_data_path,file)
            json_path = os.path.join(raw_data_path,f'{country_code}_category_id.json')

            print(f'Processing: {csv_path} + {json_path}')

            # Convert to Pandas DataFrame with known encodings
            df = read_csv_with_fallback_encoding(csv_path)

            # Mapping category_id -> category_name
            category_map = load_category_mapping(json_path)

            # Add country_code to check which country
            df['country_code'] = country_code

            # Add type of videos base on category_id
            df['category_name'] = df['category_id'].map(category_map)

            # Create date for today
            date = datetime.date.today().strftime('%Y_%m_%d')

            # Path to the output file
            output_file = f'{date}_{country_code}_videos.parquet'

            # Convert to parquet
            df.to_parquet(os.path.join(output_path,output_file),index=False)

extract_youtube_data()
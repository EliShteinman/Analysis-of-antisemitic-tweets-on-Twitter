from data_loader import DataHandler
from data_cleaner import DataCleaner
full_path = '/Users/lyhwstynmn/פרוייקטים/python/test/data/tweets_dataset.csv'
loader = DataHandler(full_path)
row_data = loader.load_data()
columns_to_drop = []
for col in row_data.columns:
    if col not in {'Text', 'Biased'}:
        columns_to_drop.append(col)
cleaner = DataCleaner(columns_to_drop)
clean_data = cleaner.clean(row_data)

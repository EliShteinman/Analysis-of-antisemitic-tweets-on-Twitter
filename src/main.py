import pandas as pd
from data_loader import DataHandler
from data_cleaner import DataCleaner

full_path = "/Users/lyhwstynmn/פרוייקטים/python/test/data/tweets_dataset.csv"
loader = DataHandler(full_path)
row_data = loader.load_data()
columns_to_drop = []
for col in row_data.columns:
    if col not in {"Text", "Biased"}:
        columns_to_drop.append(col)
cleaner = DataCleaner()
delete_unclassified = cleaner.delete_unclassified(row_data, ["Text"])
removing_punctuation_marks = cleaner.removing_punctuation_marks(
    delete_unclassified, ["Text"]
)
convert_to_lowercase = cleaner.convert_to_lowercase(
    removing_punctuation_marks, ["Text"]
)

clean_data = cleaner.deleting_columns(convert_to_lowercase, columns_to_drop)

clean_data.to_csv(
    "/Users/lyhwstynmn/פרוייקטים/python/test/results/tweets_dataset_cleaned.csv"
)

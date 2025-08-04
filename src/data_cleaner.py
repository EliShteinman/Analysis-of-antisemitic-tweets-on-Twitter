import pandas as pd
import string


class DataCleaner:
    @staticmethod
    def delete_unclassified(
        data: pd.DataFrame, columns_to_delete_unclassified: list[str]
    ) -> pd.DataFrame:
        delete_data = data.copy()
        delete_data = delete_data.dropna(subset=columns_to_delete_unclassified)
        return delete_data

    @staticmethod
    def convert_to_lowercase(
        data: pd.DataFrame, coloms_to_lowercase: list[str]
    ) -> pd.DataFrame:
        data = data.copy()
        for col in coloms_to_lowercase:
            data[col] = DataCleaner._conversion(data[col])
        return data

    @staticmethod
    def _conversion(ser: pd.Series) -> pd.Series:
        return ser.map(lambda t: t.lower())

    @staticmethod
    def removing_punctuation_marks(
        data: pd.DataFrame, coloms_to_removing_punctuation_marks: list[str]
    ) -> pd.DataFrame:
        data = data.copy()
        for col in coloms_to_removing_punctuation_marks:
            data[col] = data[col].map(
                lambda x: x.translate(str.maketrans("", "", string.punctuation))
            )
        return data

    @staticmethod
    def deleting_columns(
        data: pd.DataFrame, columns_to_drop: list[str]
    ) -> pd.DataFrame:
        cleaned_data = data.copy()
        cleaned_data = cleaned_data.drop(columns=columns_to_drop, errors="ignore")
        return cleaned_data

import pandas as pd


class DataCleaner:
    def __init__(
        self,
        columns_to_drop,
    ):
        self.columns_to_drop = columns_to_drop or []
        
    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        cleaned_data = data.copy()
        if self.columns_to_drop:
            cleaned_data = cleaned_data.drop(
                columns=self.columns_to_drop, errors="ignore"
            )
        return cleaned_data

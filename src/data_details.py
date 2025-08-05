import pandas as pd


class DataInformation:
    """

    """
    def __init__(self, data: pd.DataFrame):
        """

        """
        if data.empty:
            raise ValueError("Data cannot be empty.")
        self._row_data = data
        self._data_detailes = dict()
        self._num_of_colum = self._row_data.shape[1]
        self._num_of_rows = self._row_data.shape[0]
        self._columns_and_data_type = self._finding_columns_and_data_type()

    def _finding_columns_and_data_type(self) -> dict:
        temp = dict()
        for col in self._row_data.columns:
            temp[col] = self._row_data.col.dtype
        return temp
    

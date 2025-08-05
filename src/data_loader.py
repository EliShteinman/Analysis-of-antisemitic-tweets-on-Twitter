import pandas as pd


class DataHandler:

    def __init__(self, data_path: str, encoding: str = "utf-8"):
        """
        Initialize the DATA load object with the path to the CSV
        params:

        Args:
            data_path: The path to data
            encoding: The encoding for the file content
        """
        if not data_path:
            raise ValueError("Data path cannot be empty.")
        self.data_path = data_path
        self._encoding = encoding

        # Determining the DATA format
        self._loader_type = self._get_loader_type_from_path(self.data_path)

        # Determining the function we will use
        self._load_method_map = {
            "csv": self._load_csv,
        }
        self._selected_load_method = self._load_method_map[self._loader_type]

    def load_data(self) -> pd.DataFrame:
        """
        The function to load the data from the path we defined in init

        return:
            A pandas dataframe that holds the information
        """
        try:
            return self._selected_load_method()
        # There is currently no logger, so the division into errors is a bit unnecessary.
        except FileNotFoundError:
            raise
        except Exception:
            raise

    def _get_loader_type_from_path(self, path: str):
        """
        Getting the data file extension

        return:
            the extension
        """
        path_lower = path.lower()
        if path_lower.endswith(".csv"):
            return "csv"
        else:
            raise ValueError(f"Could not determine loader type for file: {path}")

    def _load_csv(self) -> pd.DataFrame:
        """
        Implements the logic for loading a CSV file.

        return:
            The data
        """
        return pd.read_csv(self.data_path, encoding=self._encoding)

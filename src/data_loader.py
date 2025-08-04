import pandas as pd


class DataHandler:
    """
    Handles loading data from various file formats based on a specified path.
    The loading strategy is determined internally based on the file extension.
    """

    def __init__(self, data_path: str, encoding: str = "utf-8"):
        """
        Initializes the DataHandler with the path to the data file.
        It automatically determines the loader type from the file extension.

        Args:
            data_path (str): The path to the data file to be loaded.
            encoding (str): The file encoding to use. Defaults to 'utf-8'.
        """
        if not data_path:
            raise ValueError("Data path cannot be empty.")

        self.data_path = data_path
        self._encoding = encoding

        # Determine loader type automatically from the file path
        self._loader_type = self._get_loader_type_from_path(data_path)

        # This internal dictionary maps a string identifier to the actual
        # private method responsible for the loading logic.
        self._load_method_map = {
            "csv": self._load_csv,
        }

        self._selected_load_method = self._load_method_map[self._loader_type]

    def load_data(self) -> pd.DataFrame:
        """
        Public method to load data from the configured path.
        This is the single entry point for all loading operations.

        Returns:
            pd.DataFrame: The loaded data.
        """
        try:
            return self._selected_load_method()
        except FileNotFoundError:
            raise
        except Exception:
            raise

    def _get_loader_type_from_path(self, path: str):
        """Determines the loader type from the file extension."""
        path_lower = path.lower()
        if path_lower.endswith(".csv"):
            return "csv"
        else:
            # Default to CSV if extension is unknown, or raise an error
            # For robustness, we'll raise an error.
            raise ValueError(f"Could not determine loader type for file: {path}")

    # --- Internal (Private) Implementation Methods ---
    # Note: they no longer need the 'path' argument

    def _load_csv(self) -> pd.DataFrame:
        """Implements the logic for loading a CSV file."""
        return pd.read_csv(self.data_path, encoding=self._encoding)

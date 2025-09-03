# src/data_details.py
import pandas as pd
from typing import Dict, Any, List


class DataInformation:
    """
    Class for extracting comprehensive information about the dataset.
    Provides statistics and insights required for the exam analysis.
    """

    def __init__(self, data: pd.DataFrame):
        """
        Initialize with the dataset.

        Args:
            data: The pandas DataFrame to analyze

        Raises:
            ValueError: If data is empty
        """
        if data.empty:
            raise ValueError("Data cannot be empty.")

        self._raw_data = data
        self._data_details = {}
        self._num_of_columns = self._raw_data.shape[1]
        self._num_of_rows = self._raw_data.shape[0]
        self._columns_and_data_types = self._find_columns_and_data_types()

    def _find_columns_and_data_types(self) -> Dict[str, str]:
        """
        Find all columns and their data types.

        Returns:
            Dict mapping column names to their data types
        """
        return {col: str(self._raw_data[col].dtype) for col in self._raw_data.columns}

    def get_basic_info(self) -> Dict[str, Any]:
        """
        Get basic information about the dataset.

        Returns:
            Dictionary with basic dataset information
        """
        return {
            'total_rows': self._num_of_rows,
            'total_columns': self._num_of_columns,
            'columns': list(self._raw_data.columns),
            'data_types': self._columns_and_data_types,
            'memory_usage_mb': round(self._raw_data.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        }

    def get_missing_values_info(self) -> Dict[str, int]:
        """
        Get information about missing values per column.

        Returns:
            Dictionary with missing value counts per column
        """
        return self._raw_data.isnull().sum().to_dict()

    def get_column_statistics(self, column: str) -> Dict[str, Any]:
        """
        Get statistics for a specific column.

        Args:
            column: Name of the column to analyze

        Returns:
            Dictionary with column statistics

        Raises:
            KeyError: If column doesn't exist
        """
        if column not in self._raw_data.columns:
            raise KeyError(f"Column '{column}' not found in dataset")

        col_data = self._raw_data[column]

        stats = {
            'data_type': str(col_data.dtype),
            'total_values': len(col_data),
            'missing_values': col_data.isnull().sum(),
            'unique_values': col_data.nunique()
        }

        # Add specific stats based on data type
        if pd.api.types.is_numeric_dtype(col_data):
            stats.update({
                'mean': col_data.mean(),
                'median': col_data.median(),
                'std': col_data.std(),
                'min': col_data.min(),
                'max': col_data.max()
            })
        elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
            # For text columns, add text-specific stats
            non_null_text = col_data.dropna().astype(str)
            if len(non_null_text) > 0:
                word_counts = non_null_text.apply(lambda x: len(x.split()))
                stats.update({
                    'avg_word_count': word_counts.mean(),
                    'avg_char_count': non_null_text.apply(len).mean(),
                    'most_common_values': col_data.value_counts().head(5).to_dict()
                })

        return stats

    def print_summary(self) -> None:
        """
        Print a comprehensive summary of the dataset.
        """
        print("=" * 60)
        print("DATASET INFORMATION SUMMARY")
        print("=" * 60)

        basic_info = self.get_basic_info()
        print(f"📊 Shape: {self._num_of_rows:,} rows × {self._num_of_columns} columns")
        print(f"💾 Memory Usage: {basic_info['memory_usage_mb']} MB")

        print(f"\n📋 Columns:")
        for col, dtype in self._columns_and_data_types.items():
            missing = self._raw_data[col].isnull().sum()
            missing_pct = (missing / len(self._raw_data)) * 100
            print(f"  • {col:<20} ({dtype:<10}) - {missing:,} missing ({missing_pct:.1f}%)")

        print("=" * 60)
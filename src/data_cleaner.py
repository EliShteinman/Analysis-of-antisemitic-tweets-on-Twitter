# src/data_cleaner.py
import pandas as pd
import string
import re
from typing import List


class DataCleaner:
    """
    Enhanced data cleaning class with better methods and error handling.
    Implements all cleaning operations required by the exam.
    """

    @staticmethod
    def delete_unclassified(
            data: pd.DataFrame, columns_to_delete_unclassified: List[str]
    ) -> pd.DataFrame:
        """
        Remove rows with missing values in specified columns.

        Args:
            data: Input DataFrame
            columns_to_delete_unclassified: Columns to check for missing values

        Returns:
            DataFrame without unclassified entries
        """
        if not columns_to_delete_unclassified:
            return data.copy()

        cleaned_data = data.copy()

        # Check which columns exist
        existing_columns = [col for col in columns_to_delete_unclassified if col in data.columns]

        if not existing_columns:
            print(f"⚠️ Warning: None of the specified columns found: {columns_to_delete_unclassified}")
            return cleaned_data

        # Remove rows with NaN in specified columns
        cleaned_data = cleaned_data.dropna(subset=existing_columns)

        return cleaned_data

    @staticmethod
    def convert_to_lowercase(
            data: pd.DataFrame, columns_to_lowercase: List[str]
    ) -> pd.DataFrame:
        """
        Convert text in specified columns to lowercase.

        Args:
            data: Input DataFrame
            columns_to_lowercase: List of columns to convert

        Returns:
            DataFrame with lowercase text
        """
        cleaned_data = data.copy()

        for col in columns_to_lowercase:
            if col not in cleaned_data.columns:
                print(f"⚠️ Warning: Column '{col}' not found, skipping lowercase conversion")
                continue

            # Convert to string first, then lowercase
            cleaned_data[col] = cleaned_data[col].astype(str).apply(
                lambda x: DataCleaner._safe_lowercase(x)
            )

        return cleaned_data

    @staticmethod
    def _safe_lowercase(text: str) -> str:
        """
        Safely convert text to lowercase, handling special cases.

        Args:
            text: Input text

        Returns:
            Lowercase text
        """
        if pd.isna(text) or text == 'nan':
            return ''
        return str(text).lower()

    @staticmethod
    def removing_punctuation_marks(
            data: pd.DataFrame, columns_to_remove_punctuation: List[str]
    ) -> pd.DataFrame:
        """
        Remove punctuation from specified text columns.

        Args:
            data: Input DataFrame
            columns_to_remove_punctuation: List of columns to process

        Returns:
            DataFrame with punctuation removed
        """
        cleaned_data = data.copy()

        # Create translation table for punctuation removal
        translator = str.maketrans('', '', string.punctuation)

        for col in columns_to_remove_punctuation:
            if col not in cleaned_data.columns:
                print(f"⚠️ Warning: Column '{col}' not found, skipping punctuation removal")
                continue

            # Apply punctuation removal
            cleaned_data[col] = cleaned_data[col].astype(str).apply(
                lambda x: DataCleaner._safe_remove_punctuation(x, translator)
            )

        return cleaned_data

    @staticmethod
    def _safe_remove_punctuation(text: str, translator) -> str:
        """
        Safely remove punctuation from text.

        Args:
            text: Input text
            translator: String translation table

        Returns:
            Text without punctuation
        """
        if pd.isna(text) or text == 'nan':
            return ''
        return str(text).translate(translator)

    @staticmethod
    def deleting_columns(
            data: pd.DataFrame, columns_to_drop: List[str]
    ) -> pd.DataFrame:
        """
        Remove specified columns from DataFrame.

        Args:
            data: Input DataFrame
            columns_to_drop: List of columns to remove

        Returns:
            DataFrame without specified columns
        """
        cleaned_data = data.copy()

        # Check which columns actually exist
        existing_columns_to_drop = [col for col in columns_to_drop if col in data.columns]
        non_existing_columns = [col for col in columns_to_drop if col not in data.columns]

        if non_existing_columns:
            print(f"⚠️ Warning: Columns not found (skipping): {non_existing_columns}")

        if existing_columns_to_drop:
            cleaned_data = cleaned_data.drop(columns=existing_columns_to_drop, errors='ignore')
            print(f"✅ Dropped columns: {existing_columns_to_drop}")

        return cleaned_data

    @staticmethod
    def remove_extra_whitespace(
            data: pd.DataFrame, columns_to_clean: List[str]
    ) -> pd.DataFrame:
        """
        Remove extra whitespace from text columns.

        Args:
            data: Input DataFrame
            columns_to_clean: List of columns to clean

        Returns:
            DataFrame with normalized whitespace
        """
        cleaned_data = data.copy()

        for col in columns_to_clean:
            if col not in cleaned_data.columns:
                print(f"⚠️ Warning: Column '{col}' not found, skipping whitespace cleaning")
                continue

            # Remove extra whitespace and strip
            cleaned_data[col] = cleaned_data[col].astype(str).apply(
                lambda x: DataCleaner._clean_whitespace(x)
            )

        return cleaned_data

    @staticmethod
    def _clean_whitespace(text: str) -> str:
        """
        Clean whitespace from text.

        Args:
            text: Input text

        Returns:
            Text with normalized whitespace
        """
        if pd.isna(text) or text == 'nan':
            return ''

        # Replace multiple whitespace with single space and strip
        cleaned = re.sub(r'\s+', ' ', str(text)).strip()
        return cleaned

    @staticmethod
    def remove_empty_entries(
            data: pd.DataFrame, columns_to_check: List[str]
    ) -> pd.DataFrame:
        """
        Remove rows where specified columns are empty or contain only whitespace.

        Args:
            data: Input DataFrame
            columns_to_check: Columns to check for empty content

        Returns:
            DataFrame without empty entries
        """
        cleaned_data = data.copy()

        for col in columns_to_check:
            if col not in cleaned_data.columns:
                continue

            # Remove rows where the column is empty or whitespace only
            mask = (
                    cleaned_data[col].notna() &
                    (cleaned_data[col].astype(str).str.strip() != '') &
                    (cleaned_data[col].astype(str) != 'nan')
            )
            cleaned_data = cleaned_data[mask]

        return cleaned_data

    @staticmethod
    def comprehensive_text_cleaning(
            data: pd.DataFrame,
            text_columns: List[str],
            classification_columns: List[str] = None,
            remove_unclassified: bool = True
    ) -> pd.DataFrame:
        """
        Apply comprehensive text cleaning pipeline.

        Args:
            data: Input DataFrame
            text_columns: List of text columns to clean
            classification_columns: Columns to check for classification
            remove_unclassified: Whether to remove unclassified entries

        Returns:
            Comprehensively cleaned DataFrame
        """
        print("🧹 Starting comprehensive text cleaning...")

        cleaned_data = data.copy()
        original_rows = len(cleaned_data)

        # Step 1: Remove unclassified if requested
        if remove_unclassified and classification_columns:
            print(f"  📋 Removing unclassified entries...")
            cleaned_data = DataCleaner.delete_unclassified(cleaned_data, classification_columns)
            removed_unclassified = original_rows - len(cleaned_data)
            if removed_unclassified > 0:
                print(f"    ✂️ Removed {removed_unclassified:,} unclassified rows")

        # Step 2: Remove punctuation from text columns
        print(f"  🔤 Removing punctuation...")
        cleaned_data = DataCleaner.removing_punctuation_marks(cleaned_data, text_columns)

        # Step 3: Convert to lowercase
        print(f"  🔡 Converting to lowercase...")
        cleaned_data = DataCleaner.convert_to_lowercase(cleaned_data, text_columns)

        # Step 4: Clean whitespace
        print(f"  🧽 Cleaning whitespace...")
        cleaned_data = DataCleaner.remove_extra_whitespace(cleaned_data, text_columns)

        # Step 5: Remove empty entries
        print(f"  🗑️ Removing empty entries...")
        before_empty_removal = len(cleaned_data)
        cleaned_data = DataCleaner.remove_empty_entries(cleaned_data, text_columns)
        removed_empty = before_empty_removal - len(cleaned_data)
        if removed_empty > 0:
            print(f"    ✂️ Removed {removed_empty:,} empty text rows")

        final_rows = len(cleaned_data)
        total_removed = original_rows - final_rows

        print(f"✅ Cleaning completed:")
        print(f"    📊 Original: {original_rows:,} rows")
        print(f"    📊 Final: {final_rows:,} rows")
        print(f"    📉 Removed: {total_removed:,} rows ({total_removed / original_rows * 100:.1f}%)")

        return cleaned_data
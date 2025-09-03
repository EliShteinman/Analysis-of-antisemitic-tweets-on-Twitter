# src/main.py - FIXED VERSION
"""
Complete Twitter Antisemitism Analysis Script - FIXED PATHS
Implements all requirements from the exam specification
"""

import pandas as pd
import json
import string
import re
import os
from pathlib import Path
from collections import Counter
from typing import Dict, List, Any, Union

from data_loader import DataHandler
from data_cleaner import DataCleaner
from data_details import DataInformation


class TwitterAnalysisComplete:
    """
    Complete implementation of Twitter antisemitism analysis.
    Handles all exam requirements: exploration, cleaning, and results export.
    """

    def __init__(self, data_path: str, output_dir: str = None):
        """
        Initialize the complete analysis pipeline.

        Args:
            data_path: Path to the tweets CSV file
            output_dir: Directory to save results (defaults to project/results)
        """
        # Get the project root directory (parent of src)
        current_dir = Path(__file__).parent
        project_root = current_dir.parent

        self.data_path = Path(data_path)

        # Default output directory in project root
        if output_dir is None:
            self.output_dir = project_root / "results"
        else:
            self.output_dir = Path(output_dir)

        # Create output directory if it doesn't exist
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            print(f"✅ Output directory created/verified: {self.output_dir}")
        except Exception as e:
            print(f"❌ Failed to create output directory: {e}")
            # Fallback to current directory
            self.output_dir = current_dir / "results"
            self.output_dir.mkdir(exist_ok=True)
            print(f"📁 Using fallback directory: {self.output_dir}")

        # Data storage
        self.raw_data = None
        self.cleaned_data = None
        self.analysis_results = {}

        # Column mappings (flexible for different CSV structures)
        self.text_column = "Text"
        self.classification_column = "Biased"

        print(f"🚀 Twitter Analysis initialized")
        print(f"📁 Data path: {self.data_path}")
        print(f"📁 Output directory: {self.output_dir}")

    def step1_data_exploration(self) -> Dict[str, Any]:
        """
        Step 1: Complete data exploration as required by exam.

        Returns:
            Dict with all exploration results
        """
        print("\n" + "=" * 50)
        print("STEP 1: DATA EXPLORATION")
        print("=" * 50)

        # Load data
        loader = DataHandler(str(self.data_path))
        self.raw_data = loader.load_data()

        # Basic info
        data_info = DataInformation(self.raw_data)
        data_info.print_summary()

        exploration_results = {}

        # 1.1 Count tweets by category
        print(f"\n📊 1.1 Tweet counts by category:")
        tweet_counts = self._count_tweets_by_category()
        exploration_results['total_tweets'] = tweet_counts

        for category, count in tweet_counts.items():
            if category == 'total':
                print(f"  📈 Total tweets: {count:,}")
            elif category == 'unspecified':
                if count > 0:
                    print(f"  ❓ Unclassified: {count:,}")
            else:
                category_name = "Antisemitic" if category == '1' else "Non-antisemitic"
                print(f"  • {category_name}: {count:,}")

        # 1.2 Average length analysis
        print(f"\n📝 1.2 Average length analysis (words):")
        avg_lengths = self._calculate_average_lengths()
        exploration_results['average_length'] = avg_lengths

        for category, length in avg_lengths.items():
            if category == 'total':
                print(f"  📈 Overall average: {length:.2f} words")
            else:
                category_name = "Antisemitic" if category == '1' else "Non-antisemitic"
                print(f"  • {category_name}: {length:.2f} words")

        # 1.3 Longest tweets by category
        print(f"\n📏 1.3 Longest tweets by category:")
        longest_tweets = self._find_longest_tweets()
        exploration_results['longest_3_tweets'] = longest_tweets

        for category, tweets in longest_tweets.items():
            category_name = "Antisemitic" if category == '1' else "Non-antisemitic"
            print(f"  📋 {category_name} - longest tweets:")
            for i, tweet in enumerate(tweets[:3], 1):
                preview = tweet[:100] + "..." if len(tweet) > 100 else tweet
                print(f"    {i}. {preview}")

        # 1.4 Most common words
        print(f"\n🔤 1.4 Most common words (top 10):")
        common_words = self._find_common_words()
        exploration_results['common_words'] = common_words
        print(f"  {', '.join(common_words)}")

        # 1.5 Uppercase words count
        print(f"\n📢 1.5 Uppercase words (shouting indicators):")
        uppercase_counts = self._count_uppercase_words()
        exploration_results['uppercase_words'] = uppercase_counts

        for category, count in uppercase_counts.items():
            if category == 'total':
                print(f"  📈 Total uppercase words: {count:,}")
            else:
                category_name = "Antisemitic" if category == '1' else "Non-antisemitic"
                print(f"  • {category_name}: {count:,}")

        self.analysis_results = exploration_results
        print(f"\n✅ Data exploration completed!")

        return exploration_results

    def step2_data_cleaning(self) -> pd.DataFrame:
        """
        Step 2: Clean the data according to exam requirements.

        Returns:
            Cleaned DataFrame
        """
        print("\n" + "=" * 50)
        print("STEP 2: DATA CLEANING")
        print("=" * 50)

        if self.raw_data is None:
            raise ValueError("Must run step1_data_exploration first")

        print(f"📊 Original data: {len(self.raw_data):,} rows, {len(self.raw_data.columns)} columns")

        # Initialize cleaner
        cleaner = DataCleaner()

        # Keep only relevant columns as required by exam
        columns_to_keep = [self.text_column, self.classification_column]
        available_columns = [col for col in columns_to_keep if col in self.raw_data.columns]

        if len(available_columns) != len(columns_to_keep):
            missing = set(columns_to_keep) - set(available_columns)
            print(f"⚠️ Missing columns: {missing}")
            raise ValueError(f"Required columns missing: {missing}")

        print(f"🔽 Keeping only relevant columns: {columns_to_keep}")
        cleaned_data = self.raw_data[available_columns].copy()
        print(f"   Reduced from {len(self.raw_data.columns)} to {len(cleaned_data.columns)} columns")

        # Remove unclassified tweets
        print(f"🧹 Removing unclassified tweets...")
        original_count = len(cleaned_data)
        cleaned_data = cleaner.delete_unclassified(cleaned_data, [self.classification_column])
        removed_count = original_count - len(cleaned_data)
        print(f"  Removed {removed_count:,} unclassified tweets")

        # Clean text: remove punctuation
        print(f"🧹 Removing punctuation...")
        cleaned_data = cleaner.removing_punctuation_marks(cleaned_data, [self.text_column])

        # Convert to lowercase
        print(f"🧹 Converting to lowercase...")
        cleaned_data = cleaner.convert_to_lowercase(cleaned_data, [self.text_column])

        # Final cleanup: remove extra whitespace
        print(f"🧹 Final whitespace cleanup...")
        cleaned_data[self.text_column] = cleaned_data[self.text_column].apply(
            lambda x: ' '.join(x.split()) if pd.notna(x) else x
        )

        # Remove empty text entries
        before_empty_removal = len(cleaned_data)
        cleaned_data = cleaned_data[cleaned_data[self.text_column].str.strip() != '']
        empty_removed = before_empty_removal - len(cleaned_data)
        if empty_removed > 0:
            print(f"  Removed {empty_removed:,} empty text entries")

        # Final verification - ensure we have exactly 2 columns
        final_columns = list(cleaned_data.columns)
        print(f"📋 Final cleaned data structure:")
        print(f"  • Columns: {final_columns}")
        print(f"  • Shape: {cleaned_data.shape}")

        if len(final_columns) != 2:
            print(f"⚠️ Warning: Expected 2 columns, got {len(final_columns)}")

        self.cleaned_data = cleaned_data

        print(f"📊 Final cleaned data: {len(cleaned_data):,} rows, {len(cleaned_data.columns)} columns")
        print(f"📉 Total removed: {len(self.raw_data) - len(cleaned_data):,} rows")
        print(f"✅ Data cleaning completed!")

        return cleaned_data

    def step3_export_results(self) -> None:
        """
        Step 3: Export cleaned data and analysis results.
        """
        print("\n" + "=" * 50)
        print("STEP 3: EXPORTING RESULTS")
        print("=" * 50)

        if self.cleaned_data is None:
            raise ValueError("Must run step2_data_cleaning first")

        # Export cleaned CSV
        cleaned_csv_path = self.output_dir / "tweets_dataset_cleaned.csv"
        try:
            self.cleaned_data.to_csv(cleaned_csv_path, index=False)
            print(f"💾 Cleaned dataset saved: {cleaned_csv_path}")
            print(f"   📊 {len(self.cleaned_data):,} rows, {len(self.cleaned_data.columns)} columns")
        except Exception as e:
            print(f"❌ Failed to save cleaned CSV: {e}")

        # Convert to correct format for exam requirements
        correct_format_results = self._convert_to_exam_format(self.analysis_results)

        # Export analysis results JSON in correct format
        results_json_path = self.output_dir / "results.json"
        try:
            with open(results_json_path, 'w', encoding='utf-8') as f:
                json.dump(correct_format_results, f, indent=4, ensure_ascii=False)
            print(f"💾 Analysis results saved: {results_json_path}")
        except Exception as e:
            print(f"❌ Failed to save results JSON: {e}")

        # Display final results summary
        self._print_final_summary()

        print(f"✅ Export completed!")

    def _convert_to_exam_format(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert analysis data to the EXACT format required by exam.

        Args:
            analysis_data: Raw analysis results

        Returns:
            Results in exam-required format
        """

        def convert_category_name(category_key):
            if category_key == '1':
                return 'antisemitic'
            elif category_key == '0':
                return 'non_antisemitic'
            else:
                return category_key

        correct_format = {}

        # 1. Total tweets - convert format
        if 'total_tweets' in analysis_data:
            tweet_counts = analysis_data['total_tweets']
            correct_format['total_tweets'] = {}

            for key, value in tweet_counts.items():
                if key in ['1', '0']:
                    new_key = convert_category_name(key)
                    correct_format['total_tweets'][new_key] = value
                elif key == 'unspecified':
                    correct_format['total_tweets']['unspecified'] = value
                elif key == 'total':
                    correct_format['total_tweets']['total'] = value

        # 2. Average length - convert format and round
        if 'average_length' in analysis_data:
            avg_lengths = analysis_data['average_length']
            correct_format['average_length'] = {}

            for key, value in avg_lengths.items():
                if key in ['1', '0']:
                    new_key = convert_category_name(key)
                    correct_format['average_length'][new_key] = round(value, 1)
                elif key == 'total':
                    correct_format['average_length']['total'] = round(value, 1)

        # 3. Common words - IMPORTANT: exam wants it wrapped in "total" key
        if 'common_words' in analysis_data:
            correct_format['common_words'] = {
                "total": analysis_data['common_words']
            }

        # 4. Longest tweets - convert format
        if 'longest_3_tweets' in analysis_data:
            longest_tweets = analysis_data['longest_3_tweets']
            correct_format['longest_3_tweets'] = {}

            for key, value in longest_tweets.items():
                if key in ['1', '0']:
                    new_key = convert_category_name(key)
                    correct_format['longest_3_tweets'][new_key] = value

        # 5. Uppercase words - convert format
        if 'uppercase_words' in analysis_data:
            uppercase_counts = analysis_data['uppercase_words']
            correct_format['uppercase_words'] = {}

            for key, value in uppercase_counts.items():
                if key in ['1', '0']:
                    new_key = convert_category_name(key)
                    correct_format['uppercase_words'][new_key] = value
                elif key == 'total':
                    correct_format['uppercase_words']['total'] = value

        return correct_format

    def run_complete_analysis(self) -> Dict[str, Any]:
        """
        Run the complete analysis pipeline (all 3 steps).

        Returns:
            Complete analysis results
        """
        print("🎯 STARTING COMPLETE TWITTER ANALYSIS")

        try:
            # Run all steps
            results = self.step1_data_exploration()
            self.step2_data_cleaning()
            self.step3_export_results()

            print("\n🎉 ANALYSIS COMPLETED SUCCESSFULLY!")
            return results

        except Exception as e:
            print(f"\n❌ ANALYSIS FAILED: {e}")
            raise

    def _count_tweets_by_category(self) -> Dict[str, int]:
        """Count tweets by classification category."""
        counts = self.raw_data[self.classification_column].value_counts()
        unspecified = self.raw_data[self.classification_column].isnull().sum()

        result = {}
        for category, count in counts.items():
            result[str(category)] = int(count)

        result['total'] = int(len(self.raw_data))
        result['unspecified'] = int(unspecified)

        return result

    def _calculate_average_lengths(self) -> Dict[str, float]:
        """Calculate average text length by category (in words)."""
        # Add word count column
        self.raw_data['word_count'] = self.raw_data[self.text_column].apply(
            lambda x: len(str(x).split()) if pd.notna(x) else 0
        )

        result = {}

        # Calculate by category
        for category in self.raw_data[self.classification_column].dropna().unique():
            category_data = self.raw_data[self.raw_data[self.classification_column] == category]
            avg_length = category_data['word_count'].mean()
            result[str(category)] = float(avg_length)

        # Overall average
        result['total'] = float(self.raw_data['word_count'].mean())

        return result

    def _find_longest_tweets(self, top_n: int = 3) -> Dict[str, List[str]]:
        """Find longest tweets by category."""
        if 'word_count' not in self.raw_data.columns:
            self.raw_data['word_count'] = self.raw_data[self.text_column].apply(
                lambda x: len(str(x).split()) if pd.notna(x) else 0
            )

        result = {}

        for category in self.raw_data[self.classification_column].dropna().unique():
            category_data = self.raw_data[self.raw_data[self.classification_column] == category].copy()
            longest_tweets = category_data.nlargest(top_n, 'word_count')[self.text_column].tolist()
            result[str(category)] = longest_tweets

        return result

    def _find_common_words(self, top_n: int = 10) -> List[str]:
        """Find most common words across all texts."""
        # Combine all text
        all_text = ' '.join(
            self.raw_data[self.text_column].dropna().astype(str)
        ).lower()

        # Remove punctuation and split
        translator = str.maketrans('', '', string.punctuation)
        clean_text = all_text.translate(translator)
        words = clean_text.split()

        # Filter words (minimum length 2, alphabetic only)
        filtered_words = [word for word in words if len(word) >= 2 and word.isalpha()]

        # Count and return top N
        word_counts = Counter(filtered_words)
        return [word for word, count in word_counts.most_common(top_n)]

    def _count_uppercase_words(self) -> Dict[str, int]:
        """Count words in uppercase by category."""

        def count_caps_words(text):
            if pd.isna(text):
                return 0
            words = str(text).split()
            return sum(1 for word in words if word.isupper() and len(word) > 1 and word.isalpha())

        self.raw_data['uppercase_count'] = self.raw_data[self.text_column].apply(count_caps_words)

        result = {}

        # Count by category
        for category in self.raw_data[self.classification_column].dropna().unique():
            category_data = self.raw_data[self.raw_data[self.classification_column] == category]
            total_caps = category_data['uppercase_count'].sum()
            result[str(category)] = int(total_caps)

        # Total
        result['total'] = int(self.raw_data['uppercase_count'].sum())

        return result

    def _print_final_summary(self):
        """Print final summary of results."""
        print("\n" + "=" * 60)
        print("FINAL ANALYSIS SUMMARY")
        print("=" * 60)

        tweet_counts = self.analysis_results['total_tweets']
        print(f"📊 TWEET DISTRIBUTION:")
        antisemitic_count = tweet_counts.get('1', 0)
        non_antisemitic_count = tweet_counts.get('0', 0)
        total_count = tweet_counts['total']

        print(f"  • Antisemitic: {antisemitic_count:,} ({antisemitic_count / total_count * 100:.1f}%)")
        print(f"  • Non-antisemitic: {non_antisemitic_count:,} ({non_antisemitic_count / total_count * 100:.1f}%)")
        print(f"  • Total: {total_count:,}")

        avg_lengths = self.analysis_results['average_length']
        print(f"\n📝 AVERAGE LENGTH:")
        print(f"  • Antisemitic: {avg_lengths.get('1', 0):.1f} words")
        print(f"  • Non-antisemitic: {avg_lengths.get('0', 0):.1f} words")
        print(f"  • Overall: {avg_lengths['total']:.1f} words")

        uppercase_counts = self.analysis_results['uppercase_words']
        print(f"\n📢 UPPERCASE WORDS:")
        print(f"  • Antisemitic: {uppercase_counts.get('1', 0):,}")
        print(f"  • Non-antisemitic: {uppercase_counts.get('0', 0):,}")
        print(f"  • Total: {uppercase_counts['total']:,}")

        print(f"\n🔤 TOP COMMON WORDS:")
        common_words = self.analysis_results['common_words']
        print(f"  {', '.join(common_words[:10])}")

        print("=" * 60)


def main():
    """
    Main function to run the Twitter analysis.
    Update the DATA_PATH variable to match your file location.
    """
    # 🚨 UPDATE THIS PATH TO MATCH YOUR FILE LOCATION
    DATA_PATH = "../data/tweets_dataset.csv"  # Relative path from src directory

    # Try absolute path if relative doesn't work
    current_dir = Path(__file__).parent
    project_root = current_dir.parent
    absolute_data_path = project_root / "data" / "tweets_dataset.csv"

    # Check which path exists
    if Path(DATA_PATH).exists():
        data_file_path = DATA_PATH
    elif absolute_data_path.exists():
        data_file_path = str(absolute_data_path)
    else:
        print("❌ ERROR: Data file not found!")
        print(f"Tried relative path: {Path(DATA_PATH).absolute()}")
        print(f"Tried absolute path: {absolute_data_path}")
        print("\nPlease check your data file location and update the path.")
        return None

    try:
        # Create analysis instance (output will be in project/results)
        analyzer = TwitterAnalysisComplete(
            data_path=data_file_path,
            output_dir=None  # Uses default: project/results
        )

        # Run complete analysis
        results = analyzer.run_complete_analysis()

        return results

    except Exception as e:
        print(f"❌ ERROR: Analysis failed - {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run the analysis
    main()
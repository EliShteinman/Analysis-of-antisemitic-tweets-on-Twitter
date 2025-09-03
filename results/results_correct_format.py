# results/results_correct_format.py
"""
This script shows the EXACT format required by the exam for results.json
Based on the exam specification template.
"""

import json


def create_correct_results_format(analysis_data):
    """
    Convert analysis data to the EXACT format required by exam.

    Expected format from exam:
    {
        "total_tweets": {
            "antisemitic": 512,
            "non_antisemitic": 741,
            "total": 12800,
            "unspecified": 27
        },
        "average_length": {
            "antisemitic": 18.3,
            "non_antisemitic": 20.7,
            "total": 24.2
        },
        "common_words": {
            "total": ["the","and","is","to","jews","support","zionists","peace","fight","truth"]
        },
        "longest_3_tweets": {
            "antisemitic": [...],
            "non_antisemitic": [...]
        },
        "uppercase_words": {
            "antisemitic": 139,
            "non_antisemitic": 86,
            "total": 421
        }
    }
    """

    # Convert the numbered categories to named categories
    def convert_category_name(category_key):
        if category_key == '1':
            return 'antisemitic'
        elif category_key == '0':
            return 'non_antisemitic'
        else:
            return category_key

    # Build the correct format
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

    # 2. Average length - convert format
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


def save_results_correct_format(analysis_results, output_path="results/results.json"):
    """
    Save results in the exact format required by the exam.

    Args:
        analysis_results: Results from analysis
        output_path: Path to save the JSON file
    """
    # Convert to correct format
    correct_results = create_correct_results_format(analysis_results)

    # Save with proper formatting
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(correct_results, f, indent=4, ensure_ascii=False)

    print(f"✅ Results saved in correct format: {output_path}")

    # Show a preview
    print("📋 Results preview:")
    print(json.dumps(correct_results, indent=2)[:500] + "...")

    return correct_results


# Example usage and verification
if __name__ == "__main__":
    # Example data from your current results
    example_analysis_data = {
        "total_tweets": {
            "0": 5691,
            "1": 1250,
            "total": 6941,
            "unspecified": 0
        },
        "average_length": {
            "0": 29.33614479001933,
            "1": 23.6072,
            "total": 28.304422993804927
        },
        "common_words": [
            "the", "to", "of", "and", "Jews", "in", "a", "is", "for", "are"
        ],
        "longest_3_tweets": {
            "0": ["First long non-antisemitic tweet...", "Second long...", "Third long..."],
            "1": ["First long antisemitic tweet...", "Second long...", "Third long..."]
        },
        "uppercase_words": {
            "0": 1173,
            "1": 192,
            "total": 1365
        }
    }

    # Convert and save
    correct_results = save_results_correct_format(example_analysis_data)

    # Verify the format
    print("\n🔍 Format verification:")
    required_keys = ["total_tweets", "average_length", "common_words", "longest_3_tweets", "uppercase_words"]
    for key in required_keys:
        if key in correct_results:
            print(f"  ✅ {key}: Present")
        else:
            print(f"  ❌ {key}: Missing")

    # Check specific nested structures
    if "total_tweets" in correct_results:
        tweet_keys = correct_results["total_tweets"].keys()
        expected_tweet_keys = ["antisemitic", "non_antisemitic", "total", "unspecified"]
        for key in expected_tweet_keys:
            if key in tweet_keys:
                print(f"  ✅ total_tweets.{key}: Present")
            else:
                print(f"  ❌ total_tweets.{key}: Missing")

    if "common_words" in correct_results:
        if "total" in correct_results["common_words"]:
            print(f"  ✅ common_words.total: Present (array of {len(correct_results['common_words']['total'])} words)")
        else:
            print(f"  ❌ common_words.total: Missing")
import json
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np

def analyze_json_data(file_path):
    # Read JSON file
    with open(file_path, 'r') as f:
        data = json.load(f)

    windows = data['windows']
    
    # Initialize variables
    total_bigram_counts = defaultdict(int)
    window_sentence_counts = []
    window_numbers = []

    # Process each window
    for window in windows:
        window_numbers.append(window['window_number'])
        window_sentence_counts.append(window['sentence_count'])
        
        # Sum up bigram counts
        for bigram_data in window['bigrams']:
            bigram = tuple(bigram_data['bigram'])
            count = bigram_data['count']
            total_bigram_counts[bigram] += count

    # Calculate running average
    running_average = [sum(window_sentence_counts[:i+1]) / (i+1) for i in range(len(window_sentence_counts))]

    # Print total bigram counts
    print("Total Bigram Counts:")
    for bigram, count in sorted(total_bigram_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {bigram}: {count}")

    # Print metrics
    print("\nMetrics:")
    print(f"Total number of windows: {len(window_numbers)}")
    print(f"Total number of sentences: {sum(window_sentence_counts)}")
    print(f"Average sentences per window: {np.mean(window_sentence_counts):.2f}")
    print(f"Median sentences per window: {np.median(window_sentence_counts):.2f}")
    print(f"Minimum sentences in a window: {min(window_sentence_counts)}")
    print(f"Maximum sentences in a window: {max(window_sentence_counts)}")
    print(f"Standard deviation of sentences per window: {np.std(window_sentence_counts):.2f}")
    
    # Print running average metrics
    print("\nRunning Average Metrics:")
    print(f"Final running average: {running_average[-1]:.2f}")
    print(f"Minimum running average: {min(running_average):.2f}")
    print(f"Maximum running average: {max(running_average):.2f}")

    # Calculate linear regression
    x = np.array(window_numbers)
    y = np.array(window_sentence_counts)
    m, b = np.polyfit(x, y, 1)
    trend_line = m * x + b

    print("\nTrend Line:")
    print(f"Slope: {m:.4f}")
    print(f"Y-intercept: {b:.4f}")
    print(f"Equation: y = {m:.4f}x + {b:.4f}")

    # Create plot
    plt.figure(figsize=(12, 6))
    plt.plot(window_numbers, window_sentence_counts, marker='o', label='Sentence Count')
    plt.plot(window_numbers, running_average, linestyle='--', color='red', label='Running Average')
    plt.plot(window_numbers, trend_line, linestyle=':', color='green', label='Trend Line')
    plt.xlabel('Window Number')
    plt.ylabel('Number of Sentences')
    plt.title('Sentence Count per Window')
    plt.legend()
    plt.grid(True)
    
    # Add text annotations
    plt.text(0.02, 0.98, f"Total windows: {len(window_numbers)}", transform=plt.gca().transAxes, verticalalignment='top')
    plt.text(0.02, 0.94, f"Avg sentences: {np.mean(window_sentence_counts):.2f}", transform=plt.gca().transAxes, verticalalignment='top')
    plt.text(0.02, 0.90, f"Std dev: {np.std(window_sentence_counts):.2f}", transform=plt.gca().transAxes, verticalalignment='top')
    
    plt.savefig('/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_4/src/output/window_sentence_count.png')
    plt.close()

    print("\nGraph saved as 'window_sentence_count.png'")

if __name__ == "__main__":
    json_file_path = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_4/src/output/output_log.json"  
    analyze_json_data(json_file_path)
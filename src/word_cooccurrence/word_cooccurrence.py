import sys
import re
import subprocess
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import logging
import warnings
import json

# I want to keep my log output clean, so I'm suppressing INFO and WARNING logs from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# Suppressing specific warnings related to psutil which are not relevant for my current work
warnings.filterwarnings("ignore", category=UserWarning, message=".*psutil.*")

class WC:

    class Utils:
        @staticmethod
        def proc_line(line):
            # This method processes each line by converting it to lowercase and extracting words using regex.
            # The '\w+' pattern matches sequences of word characters (letters, digits, and underscores).
            words = re.findall(r'\w+', line.lower())
            return words

        @staticmethod
        def gen_bigrams(words):
            # This method generates bigrams (pairs of consecutive words) from a list of words.
            # It iterates through the words list, pairing each word with the next one.
            return [(words[i], words[i + 1]) for i in range(len(words) - 1)]

    class Log:
        @staticmethod
        def log_lines(time, rdd):
            # This method logs the lines received in each RDD along with the timestamp.
            # It first counts the number of lines, collects them into a list, and then prints each line.
            line_count = rdd.count()
            lines = rdd.collect()
            print(f"Received {line_count} lines at time {time}:")
            for line in lines:
                print(line)

    class Proc:
        def __init__(self, sc):
            # Initializing the processor with the Spark context.
            self.sc = sc

        def proc_rdd(self, rdd):
            # This method processes each RDD to extract words and generate bigrams.
            # It collects all lines in the RDD, processes each line to extract words, and then generates bigrams from all the words.
            lines = rdd.collect()
            all_words = []
            for line in lines:
                all_words.extend(WC.Utils.proc_line(line))
            
            bigrams = WC.Utils.gen_bigrams(all_words)
            # Returning the bigrams as a new RDD
            return self.sc.parallelize(bigrams)

    class NetSetup:
        @staticmethod
        def start_nc(port):
            # This method starts the Netcat script to send data through the specified port.
            # I'm using subprocess to run the script that sends data over the network.
            netcat_script = "/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_4/src/utils/send_paragraph.sh"
            subprocess.Popen([netcat_script, str(port)])

    class SparkSetup:
        def __init__(self):
            # Initializing Spark context and Streaming context to None
            self.sc = None
            self.ssc = None

        def setup_sc(self):
            # Setting up the Spark context with application name "WC" and running in local mode with 2 threads.
            # Then, I'm initializing the Streaming context with a 1-second batch interval.
            conf = SparkConf().setAppName("WC").setMaster("local[2]")
            self.sc = SparkContext(conf=conf)
            self.ssc = StreamingContext(self.sc, 1)

    class WindowProc:
        def __init__(self, ssc, proc):
            # Initializing the Window processor with the Streaming context and the processor.
            # I'm also setting up counters and a list to keep track of processed data.
            self.ssc = ssc
            self.proc = proc
            self.window_count = 0
            self.log_data = []

        def proc_window(self, lines):
            # This method processes the data stream in a sliding window.
            # I set the window duration to 3 seconds and slide interval to 2 seconds.
            windowed_lines = lines.window(3, 2)
            # Logging data for each window
            windowed_lines.foreachRDD(self.log_window_data)
            # Transforming lines into bigrams
            bigrams = windowed_lines.transform(lambda rdd: self.proc.proc_rdd(rdd))
            # Counting the occurrences of each bigram
            bigram_counts = bigrams.map(lambda bigram: (bigram, 1)).reduceByKey(lambda x, y: x + y)
            # Logging bigram counts for each window
            bigram_counts.foreachRDD(self.log_bigram_counts)

        def log_window_data(self, time, rdd):
            # Logging data from each window of the stream.
            # Incrementing the window count and collecting lines from the RDD.
            self.window_count += 1
            lines = rdd.collect()
            # Creating a dictionary to store data for the current window
            window_data = {
                "window_number": self.window_count,
                "sentence_count": len(lines),
                "sentences": lines,
                "bigrams": []
            }
            # Adding the current window's data to the log data list
            self.log_data.append(window_data)
            
            # Printing details of the current window
            print(f"Window {self.window_count}:")
            print(f"Number of sentences: {len(lines)}")
            print("Sentences:")
            for line in lines:
                print(f"  {line}")

        def log_bigram_counts(self, time, rdd):
            # Logging the bigram counts from the RDD.
            # Collecting the bigram counts and updating the current window's data with these counts.
            bigram_counts = rdd.collect()
            current_window = self.log_data[-1]
            current_window["bigrams"] = [{"bigram": list(bigram), "count": count} for bigram, count in bigram_counts]
            
            # Printing bigram counts for the current window
            print("Bigrams and their co-occurrences count:")
            for bigram, count in bigram_counts:
                print(f"  {bigram}: {count}")
            print()

    def __init__(self, host="localhost", port=9999):
        # Initializing the main WC class with default host and port for the streaming server.
        # Also initializing instances of SparkSetup, Proc, and WindowProc classes.
        self.host = host
        self.port = port
        self.spark_setup = WC.SparkSetup()
        self.proc = None
        self.window_proc = None
        self.empty_line_count = 0

    def setup(self):
        # Setting up the network connection, Spark context, and processors.
        # Starting the Netcat script, setting up the Spark context, and initializing the processor and window processor.
        WC.NetSetup.start_nc(self.port)
        self.spark_setup.setup_sc()
        self.proc = WC.Proc(self.spark_setup.sc)
        self.window_proc = WC.WindowProc(self.spark_setup.ssc, self.proc)

    def run(self):
        # Running the Spark streaming job.
        # Setting up the environment and defining the data stream from the specified host and port.
        self.setup()

        # Creating a DStream that connects to the hostname and port.
        lines = self.spark_setup.ssc.socketTextStream(self.host, self.port)
        # Checking for empty lines in each RDD.
        lines.foreachRDD(self.check_empty_lines)

        # Processing the data stream in windows.
        self.window_proc.proc_window(lines)

        # Starting the streaming context and waiting for termination.
        self.spark_setup.ssc.start()
        self.spark_setup.ssc.awaitTermination()

    def check_empty_lines(self, time, rdd):
        # Checking for empty lines in the stream.
        # Logging the lines received, and if three consecutive batches are empty, stopping the Spark context gracefully.
        WC.Log.log_lines(time, rdd)
        lines = rdd.collect()
        if all(line.strip() == "" for line in lines):
            self.empty_line_count += 1
            if self.empty_line_count == 3:
                self.generate_log_files()
                self.spark_setup.ssc.stop(stopSparkContext=True, stopGraceFully=True)
        else:
            self.empty_line_count = 0

    def generate_log_files(self):
        # Generating JSON and text log files from the processed data.
        # Creating a dictionary with the log data and writing it to a JSON file.
        output_data = {
            "windows": self.window_proc.log_data
        }
        
        with open("/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_4/src/output/output_log.json", "w") as f:
            json.dump(output_data, f, indent=2)
        
        print("JSON log file generated: output_log.json")

        # Writing the log data to a text file in a readable format.
        with open("/Users/kritikkumar/Documents/DIC_summer_2024/shaivipa_kritikku_assignment_4/src/output/output_log.txt", "w") as f:
            for window in self.window_proc.log_data:
                f.write(f"Window {window['window_number']}:\n")
                f.write(f"Number of sentences: {window['sentence_count']}\n")
                f.write("Sentences:\n")
                for sentence in window['sentences']:
                    f.write(f"  {sentence}\n")
                f.write("Bigrams and their co-occurrences count:\n")
                for bigram in window['bigrams']:
                    f.write(f"  {tuple(bigram['bigram'])}: {bigram['count']}\n")
                f.write("\n")
        
        print("Text log file generated: output_log.txt")

if __name__ == "__main__":
    # Running the WC class if this script is executed directly.
    wc = WC()
    wc.run()

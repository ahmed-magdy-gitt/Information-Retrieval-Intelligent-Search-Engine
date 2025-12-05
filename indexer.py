import findspark
findspark.init()

import os
import sys
import shutil
from pyspark.sql import SparkSession

# --- WINDOWS & JAVA CONFIGURATION ---
# We set these inside the code to make sure Spark finds them
os.environ['JAVA_HOME'] = r"C:\Program Files\Java\jdk-21"
os.environ['HADOOP_HOME'] = r"C:\IR Project\hadoop"
sys.path.append(r"C:\IR Project\hadoop\bin") 
os.environ['PATH'] += os.pathsep + r"C:\IR Project\hadoop\bin"

def create_index():
    # 1. Start Spark Session
    # We add special configs for Java 21 compatibility
    spark = SparkSession.builder \
        .appName("IR_Project_Indexer") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR") # Reduce noise in terminal

    # --- PATHS ---
    # file:/// is required for Windows paths in Spark
    input_path = "file:///C:/IR Project/data/*.txt"
    temp_folder = "temp_spark_output"
    final_output_file = "output.txt"

    print(f"\n🚀 Starting Indexing process on: {input_path}")

    # Clean up previous runs
    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)
    if os.path.exists(final_output_file):
        os.remove(final_output_file)

    try:
        # 2. Read Files
        files_rdd = sc.wholeTextFiles(input_path)
        
        if files_rdd.isEmpty():
            print("❌ ERROR: No .txt files found in 'C:/IR Project/data/'")
            return

        # 3. Processing Logic (Map -> FlatMap -> GroupBy)
        def process_file(record):
            file_path, content = record
            doc_name = os.path.basename(file_path)
            words = content.lower().split()
            
            output = []
            for index, word in enumerate(words):
                clean_word = word.strip()
                if clean_word:
                    # Key: Term, Value: (DocName, Position)
                    output.append((clean_word, (doc_name, index + 1)))
            return output

        # Group data by Term
        grouped_rdd = files_rdd.flatMap(process_file).groupByKey()

        # Sort by Term Alphabetically
        sorted_rdd = grouped_rdd.sortByKey()

        # 4. Format Output String
        def format_output(item):
            term, doc_pos_iterable = item
            
            # Organize positions by document
            doc_map = {}
            for doc, pos in doc_pos_iterable:
                if doc not in doc_map: doc_map[doc] = []
                doc_map[doc].append(pos)
            
            # Sort documents (1.txt, 2.txt...)
            sorted_docs = sorted(doc_map.keys(), key=lambda x: int(x.split('.')[0]) if x.split('.')[0].isdigit() else x)
            
            doc_strings = []
            for doc in sorted_docs:
                positions = sorted(doc_map[doc])
                pos_str = ", ".join(map(str, positions))
                doc_strings.append(f"{doc}: {pos_str}")
            
            full_str = " ; ".join(doc_strings)
            return f"<{term} : {full_str}>"

        final_rdd = sorted_rdd.map(format_output)

        # 5. Save to temp folder
        final_rdd.coalesce(1).saveAsTextFile(temp_folder)
        
        spark.stop() # Stop Spark to release file locks

        # 6. Move and Rename file (Cleanup)
        print("✅ Spark finished. Moving output file...")
        
        part_file = None
        for filename in os.listdir(temp_folder):
            if filename.startswith("part-"):
                part_file = os.path.join(temp_folder, filename)
                break
        
        if part_file:
            shutil.move(part_file, final_output_file)
            shutil.rmtree(temp_folder) # Delete temp folder
            print(f"🎉 SUCCESS! Index generated at: {os.path.abspath(final_output_file)}")
        else:
            print("❌ ERROR: Could not find part-file.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")

if __name__ == "__main__":
    create_index()
# 🔍 Intelligent Search Engine & Information Retrieval System
### *Implementing Vector Space Model & Boolean Query Processing from Scratch*

![Python](https://img.shields.io/badge/Python-3.x-blue?style=for-the-badge&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-Integration-E25A1C?style=for-the-badge&logo=apachespark)
![Algorithm](https://img.shields.io/badge/Algorithm-TF--IDF_%26_Cosine_Similarity-green?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Completed-success?style=for-the-badge)

## 📋 Executive Summary
This project represents the core engine of an Information Retrieval system. It is designed to ingest a **Positional Inverted Index** (generated via **Apache Spark** for scalability) and perform complex search operations using the **Vector Space Model (VSM)**.

Unlike standard implementations that rely on pre-built libraries, this project implements the mathematical foundations of search engines **from first principles**. It features a custom-built Query Parser, manual TF-IDF calculation, and a Ranking System based on Cosine Similarity.

---

## ⚙️ System Architecture & Core Modules

### 1️⃣ Index Ingestion & Parsing (Big Data Integration)
The system connects with the output of a distributed indexer (Spark). It reads a raw text-based **Positional Inverted Index** and converts it into an optimized in-memory Python structure.
* **Technique:** Advanced **Regex** parsing to handle complex string structures ` < term : doc1: pos1, pos2 ; ... >`.
* **Data Structure:** Nested Dictionaries allowing for $O(1)$ access time to terms and their document positions.

### 2️⃣ The Mathematical Core (TF-IDF & VSM)
The engine transforms raw text into mathematical vectors to enable semantic ranking.
* **Term Frequency (TF):** Calculated manually based on term occurrences within documents.
* **Inverse Document Frequency (IDF):** Implemented using the logarithmic scale to penalize common words.
    $$IDF(t) = \log_{10} \left( \frac{N}{df_t} \right)$$
* **TF-IDF Matrix:** Construction of a weighted matrix representing the importance of each term across the corpus.

### 3️⃣ Advanced Query Processing Engine
A robust parser that handles complex user queries with multiple logic layers:
* **Boolean Operators:** Full support for `AND`, `NOT`, and `AND NOT` logic.
* **Phrase Queries:** Implements **Positional Search** to ensure exact phrase matching (e.g., finding "angels fools" where words appear continuously), not just "Bag of Words".
* **Algorithm:** Intersecting positional lists to verify word adjacency.

### 4️⃣ Relevance Ranking (Cosine Similarity)
Instead of just returning matching documents, the system ranks them by relevance.
* **Vector Normalization:** Calculating Euclidean Norms for document and query vectors.
* **Similarity Score:** Computing the angle between vectors to determine the best match.
    $$\text{Similarity} = \cos(\theta) = \frac{A \cdot B}{\|A\| \|B\|}$$

---

## 💻 Tech Stack
| Component | Technology |
| :--- | :--- |
| **Language** | Python 3.10+ |
| **Indexing Source** | Apache Spark (Input Provider) |
| **Text Processing** | Regular Expressions (Re) |
| **Math Operations** | Math Module (Logarithms, Sqrt) |
| **Interface** | CLI with Formatted Data Tables |

## 📊 Features Snapshot
The system outputs detailed analytical tables before the search phase:
1.  **TF Table:** Raw frequency counts.
2.  **IDF Table:** Global importance of terms.
3.  **TF-IDF Matrix:** The final vector representation.
4.  **Ranked Results:** Search results sorted by score (e.g., `Score: 0.8543`).

## 🚀 How to Run
1.  **Prerequisite:** Ensure `output.txt` (the Spark Index) exists in the root directory.
2.  **Run the Engine:**
    ```bash
    python search_engine.py
    ```
3.  **Search Examples:**
    * *Phrase Query:* `angels fools`
    * *Boolean Logic:* `antony AND brutus`
    * *Exclusion:* `rush AND NOT fear`

---
*Developed as part of the Information Retrieval Coursework | 2025*
*Focusing on Algorithmic Efficiency and Mathematical Implementation.*

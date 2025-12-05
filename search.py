import math
import sys
import os
import re

# اسم ملف الاندكس الناتج من سبارك
INPUT_FILE = "output.txt"

def load_index(file_path):
    """
    وظيفة الدالة: قراءة ملف output.txt وتحويله لقاموس بايثون
    تستخدم Regex لضمان القراءة الصحيحة وتجنب الأخطاء
    """
    print(f"Loading index from {file_path}...")
    index = {}
    all_docs = set()
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File '{file_path}' not found.")
        print("Please run the Spark Indexer first to generate this file.")
        return None, None

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            line = line.strip()
            if not line: continue
            
            # Regex لتقسيم السطر بشكل آمن:
            # يبحث عن: < term : rest_of_line >
            # النمط يتجاهل المسافات الزائدة
            match = re.match(r"^<\s*(.*?)\s*:\s*(.*)\s*>$", line)
            
            if not match:
                # محاولة تخطي السطور التالفة بدلاً من إيقاف البرنامج
                continue
                
            term = match.group(1).strip()
            docs_str = match.group(2).strip()
            
            index[term] = {}
            
            # تقسيم المستندات بفاصلة منقوطة
            # Format: doc1: pos1, pos2 ; doc2: pos1 ...
            doc_entries = docs_str.split(';')
            
            for entry in doc_entries:
                if ':' not in entry: continue
                
                doc_part, pos_part = entry.split(':', 1)
                doc_name = doc_part.strip()
                
                # استخراج الأرقام (المواقع)
                positions = [int(p) for p in re.findall(r'\d+', pos_part)]
                
                if doc_name and positions:
                    index[term][doc_name] = positions
                    all_docs.add(doc_name)
                    
    # ترتيب المستندات (1.txt, 2.txt...) لضمان شكل الجداول
    # نقوم باستخراج الرقم من اسم الملف للترتيب الصحيح
    def sort_key(doc):
        nums = re.findall(r'\d+', doc)
        return int(nums[0]) if nums else doc
        
    sorted_docs = sorted(list(all_docs), key=sort_key)
    
    print(f"✅ Successfully loaded {len(index)} terms and {len(sorted_docs)} documents.")
    return index, sorted_docs

def print_table(title, headers, rows):
    """ دالة مساعدة لطباعة الجداول بشكل منظم """
    print(f"\n📊 --- {title} ---")
    
    # حساب عرض كل عمود بناءً على أطول كلمة فيه
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(val)))
    
    # إضافة مسافة جمالية
    col_widths = [w + 2 for w in col_widths]
    
    # طباعة الرأس
    header_str = "".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    print(header_str)
    print("-" * len(header_str))
    
    # طباعة الصفوف
    for row in rows:
        print("".join(f"{str(val):<{col_widths[i]}}" for i, val in enumerate(row)))

def main():
    # 1. تحميل البيانات
    index, all_docs = load_index(INPUT_FILE)
    if index is None: return

    terms = sorted(index.keys())
    N = len(all_docs) # عدد الملفات الكلي

    # ==========================================
    # 2. حساب Term Frequency (TF)
    # ==========================================
    print("\n[1] Computing Term Frequency (TF)...")
    
    tf_matrix = {} # لتخزين القيم للاستخدام لاحقاً
    table_rows = []
    
    for term in terms:
        row = [term]
        tf_matrix[term] = {}
        for doc in all_docs:
            # TF هو عدد مرات ظهور الكلمة (عدد المواقع)
            count = len(index[term].get(doc, []))
            tf_matrix[term][doc] = count
            row.append(count)
        table_rows.append(row)
        
    print_table("Term Frequency (TF)", ["Term"] + all_docs, table_rows)

    # ==========================================
    # 3. حساب Inverse Document Frequency (IDF)
    # ==========================================
    print("\n[2] Computing IDF...")
    
    idf_dict = {}
    table_rows = []
    
    for term in terms:
        df = len(index[term]) # في كم ملف ظهرت الكلمة؟
        # القانون: log10( N / df )
        idf = math.log10(N / df) if df > 0 else 0
        idf_dict[term] = idf
        table_rows.append([term, f"{idf:.4f}"])
        
    print_table("IDF Values", ["Term", "IDF"], table_rows)

    # ==========================================
    # 4. حساب TF-IDF Matrix
    # ==========================================
    print("\n[3] Computing TF-IDF Matrix...")
    
    tf_idf_matrix = {}
    table_rows = []
    doc_norms = {doc: 0.0 for doc in all_docs} # لتجهيز Cosine Similarity
    
    for term in terms:
        row = [term]
        tf_idf_matrix[term] = {}
        for doc in all_docs:
            tf = tf_matrix[term][doc]
            idf = idf_dict[term]
            
            # القانون: TF * IDF
            val = tf * idf
            tf_idf_matrix[term][doc] = val
            row.append(f"{val:.4f}")
            
            # تجميع مربع القيم لحساب طول المتجه (Vector Norm) للملف
            doc_norms[doc] += val ** 2
            
        table_rows.append(row)
        
    # أخذ الجذر التربيعي للـ Norms
    for doc in doc_norms:
        doc_norms[doc] = math.sqrt(doc_norms[doc])
        
    print_table("TF-IDF Matrix", ["Term"] + all_docs, table_rows)

    # ==========================================
    # 5. محرك البحث (Search Engine)
    # ==========================================
    print("\n🔍 --- Search Engine Ready ---")
    print("Example queries: 'angels fools', 'antony AND brutus', 'rush AND NOT fear'")
    
    while True:
        try:
            query = input("\nQuery > ").strip()
            if query.lower() in ['exit', 'quit']:
                break
            if not query: continue
            
            # --- 5.1 تحليل الاستعلام (Parsing) ---
            must_include = []
            must_exclude = []
            
            # التعامل مع AND NOT أولاً
            if ' AND NOT ' in query:
                parts = query.split(' AND NOT ')
                must_include.append(parts[0].strip())
                must_exclude.append(parts[1].strip())
            elif ' AND ' in query:
                parts = query.split(' AND ')
                must_include.extend([p.strip() for p in parts])
            elif query.startswith('NOT '):
                must_exclude.append(query[4:].strip())
            else:
                must_include.append(query) # جملة واحدة
            
            # --- 5.2 البحث عن المستندات (Boolean Logic & Phrase) ---
            # دالة مساعدة للبحث عن جملة (Phrase Query)
            def find_phrase_docs(phrase_text):
                p_terms = phrase_text.lower().split()
                if not p_terms: return set()
                # التأكد من وجود الكلمات
                for t in p_terms:
                    if t not in index: return set()
                
                # الملفات المرشحة (التي تحتوي كل الكلمات)
                candidates = set(index[p_terms[0]].keys())
                for t in p_terms[1:]:
                    candidates &= set(index[t].keys())
                
                matched = set()
                for doc in candidates:
                    # التأكد من الترتيب (Positions)
                    curr_pos = index[p_terms[0]][doc]
                    for i in range(1, len(p_terms)):
                        next_pos = index[p_terms[i]][doc]
                        # هل يوجد موقع (سابق + 1)؟
                        curr_pos = [p+1 for p in curr_pos if (p+1) in next_pos]
                        if not curr_pos: break
                    if curr_pos: matched.add(doc)
                return matched

            # تطبيق المنطق
            result_docs = None
            
            # Include
            if must_include:
                for phrase in must_include:
                    docs = find_phrase_docs(phrase)
                    if result_docs is None: result_docs = docs
                    else: result_docs &= docs # تقاطع (AND)
            else:
                result_docs = set() # إذا كان الاستعلام NOT فقط، نفترض البحث في الكل (أو فارغ حسب المنطق)
            
            # Exclude
            for phrase in must_exclude:
                docs = find_phrase_docs(phrase)
                if result_docs is None: result_docs = set(all_docs)
                result_docs -= docs # استبعاد
                
            if not result_docs:
                print("No documents found.")
                continue

            # --- 5.3 الترتيب (Ranking - Cosine Similarity) ---
            # تجميع كلمات الاستعلام لحساب المتجه
            query_terms = []
            for phrase in must_include:
                query_terms.extend(phrase.lower().split())
            
            # حساب وزن الاستعلام
            # TF query = 1 (للتبسيط)
            q_vec = {}
            q_norm = 0
            for t in query_terms:
                if t in idf_dict:
                    w = idf_dict[t] # (1 + log(1)) * idf = idf
                    q_vec[t] = w
                    q_norm += w**2
            q_norm = math.sqrt(q_norm)
            
            # حساب Cosine Similarity
            scores = []
            for doc in result_docs:
                dot_product = 0
                for t, w in q_vec.items():
                    if t in tf_idf_matrix:
                        dot_product += w * tf_idf_matrix[t][doc]
                
                sim = 0
                if q_norm > 0 and doc_norms[doc] > 0:
                    sim = dot_product / (q_norm * doc_norms[doc])
                
                scores.append((doc, sim))
            
            # طباعة النتائج مرتبة
            scores.sort(key=lambda x: x[1], reverse=True)
            print(f"\nFound {len(scores)} documents:")
            for doc, score in scores:
                print(f"📄 {doc:<10} (Score: {score:.4f})")

        except Exception as e:
            print(f"Error processing query: {e}")

if __name__ == "__main__":
    main()
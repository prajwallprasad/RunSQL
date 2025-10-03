# import mysql.connector
# from mysql.connector import Error

# # ------------------------
# # 1. Database connection
# # ------------------------
# def get_connection():
#     """
#     Returns a MySQL connection to your Docker container
#     """
#     try:
#         conn = mysql.connector.connect(
#             host="127.0.0.1",   # your Docker container name
#             user="root",              # MySQL username
#             password="Praju@2003",   # MySQL password
#             database="RunSQL"         # your database name
#         )
#         return conn
#     except Error as e:
#         print(f"Error connecting to MySQL: {e}")
#         return None

# # ------------------------
# # 2. Log execution status
# # ------------------------
# def log_status(source_id, request, status, output):
#     """
#     Logs execution info into SQLStoreStatus table
#     """
#     conn = get_connection()
#     if not conn:
#         return

#     cursor = conn.cursor()
#     cursor.execute("""
#         INSERT INTO SQLStoreStatus (source, request, status, output)
#         VALUES (%s, %s, %s, %s)
#     """, (source_id, request, status, str(output)))  # store output as string
#     conn.commit()
#     conn.close()

# # ------------------------
# # 3. Execute SQL by ID
# # ------------------------
# def execute_query_by_id(query_id):
#     """
#     Executes a SQL statement from SQLStore by its ID
#     and logs result into SQLStoreStatus
#     """
#     conn = get_connection()
#     if not conn:
#         return {"error": "Database connection failed"}

#     cursor = conn.cursor(dictionary=True)

#     # 1. Fetch SQL statement
#     cursor.execute("SELECT * FROM SQLStore WHERE id = %s", (query_id,))
#     query_row = cursor.fetchone()
#     if not query_row:
#         conn.close()
#         return {"error": f"No query found with id {query_id}"}

#     sql = query_row["description"]

#     # 2. Execute query
#     try:
#         cursor.execute(sql)
#         if cursor.with_rows:
#             result = cursor.fetchall()
#         else:
#             conn.commit()
#             result = {"rows_affected": cursor.rowcount}

#         # 3. Log success
#         log_status(
#             source_id=query_id,
#             request=f"POST /start/{query_id}",
#             status="Success",
#             output=result
#         )

#         conn.close()
#         return {
#             "id": query_id,
#             "status": "Success",
#             "output": result
#         }

#     except Error as e:
#         # Log failure
#         log_status(
#             source_id=query_id,
#             request=f"POST /start/{query_id}",
#             status="Failed",
#             output=str(e)
#         )
#         conn.close()
#         return {"id": query_id, "status": "Failed", "error": str(e)}

# # ------------------------
# # 4. Get latest execution status
# # ------------------------
# def get_last_status(query_id):
#     """
#     Fetch the latest execution status for a given SQLStore ID
#     """
#     conn = get_connection()
#     if not conn:
#         return {"error": "Database connection failed"}

#     cursor = conn.cursor(dictionary=True)
#     cursor.execute("""
#         SELECT *
#         FROM SQLStoreStatus
#         WHERE source = %s
#         ORDER BY id DESC
#         LIMIT 1
#     """, (query_id,))
#     row = cursor.fetchone()
#     conn.close()
#     return row


import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from logger import logging

# ------------------------
# 1. Database connection
# ------------------------
def get_connection():
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",   # Docker container host
            user="root",
            password="Praju@2003",
            database="RunSQL"
        )
        return conn
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

# ------------------------
# 2. Log execution status
# ------------------------
def log_status(source_id, request, status, output):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO SQLStoreStatus (source, request, status, output)
        VALUES (%s, %s, %s, %s)
    """, (source_id, request, status, str(output)))
    conn.commit()
    conn.close()

# ------------------------
# 3. Execute SQL by ID
# ------------------------
def execute_query_by_id(query_id):
    conn = get_connection()
    if not conn:
        return {"error": "Database connection failed"}
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM SQLStore WHERE id = %s", (query_id,))
    query_row = cursor.fetchone()
    if not query_row:
        conn.close()
        return {"error": f"No query found with id {query_id}"}
    
    sql = query_row["description"]

    try:
        cursor.execute(sql)
        if cursor.with_rows:
            result = cursor.fetchall()
        else:
            conn.commit()
            result = {"rows_affected": cursor.rowcount}

        log_status(query_id, f"POST /start/{query_id}", "Success", result)
        conn.close()
        return {"id": query_id, "status": "Success", "output": result}

    except Error as e:
        log_status(query_id, f"POST /start/{query_id}", "Failed", str(e))
        conn.close()
        return {"id": query_id, "status": "Failed", "error": str(e)}

# ------------------------
# 4. Get latest execution status
# ------------------------
def get_last_status(query_id):
    conn = get_connection()
    if not conn:
        return {"error": "Database connection failed"}
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT *
        FROM SQLStoreStatus
        WHERE source = %s
        ORDER BY id DESC
        LIMIT 1
    """, (query_id,))
    row = cursor.fetchone()
    conn.close()
    return row

# ------------------------
# 5. Helper: get all query IDs
# ------------------------
def get_all_query_ids():
    conn = get_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM SQLStore")
    ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return ids

# ------------------------
# 6. Batch execution - multithreading
# ------------------------
def run_queries_multithreaded(query_ids, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(execute_query_by_id, qid) for qid in query_ids]
        for future in futures:
            results.append(future.result())
    return results

# ------------------------
# 7. Batch execution - multiprocessing
# ------------------------
def run_queries_multiprocessed(query_ids, processes=4):
    with Pool(processes=processes) as pool:
        results = pool.map(execute_query_by_id, query_ids)
    return results

# ------------------------
# 8. Batch execution - hybrid (multi-process + multi-thread)
# ------------------------
def run_queries_hybrid(query_ids, num_processes=4, threads_per_process=5):
    import multiprocessing

    def run_chunk(chunk):
        from db import run_queries_multithreaded
        return run_queries_multithreaded(chunk, threads_per_process)

    # Split query_ids into chunks
    chunk_size = len(query_ids) // num_processes
    chunks = [query_ids[i*chunk_size:(i+1)*chunk_size] for i in range(num_processes)]
    remaining = query_ids[num_processes*chunk_size:]
    for i, qid in enumerate(remaining):
        chunks[i % num_processes].append(qid)

    if __name__ == "__main__":
        with multiprocessing.Pool(processes=num_processes) as pool:
            results = pool.map(run_chunk, chunks)
    else:
        # Fallback if called from FastAPI (Windows may require spawn method)
        results = []
        for chunk in chunks:
            results.extend(run_queries_multithreaded(chunk, threads_per_process))

    # Flatten results
    flat_results = [item for sublist in results for item in sublist]
    return flat_results

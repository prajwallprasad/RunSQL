import mysql.connector
from mysql.connector import Error

# ------------------------
# 1. Database connection
# ------------------------
def get_connection():
    """
    Returns a MySQL connection to your Docker container
    """
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",   # your Docker container name
            user="root",              # MySQL username
            password="Praju@2003",   # MySQL password
            database="RunSQL"         # your database name
        )
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# ------------------------
# 2. Log execution status
# ------------------------
def log_status(source_id, request, status, output):
    """
    Logs execution info into SQLStoreStatus table
    """
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO SQLStoreStatus (source, request, status, output)
        VALUES (%s, %s, %s, %s)
    """, (source_id, request, status, str(output)))  # store output as string
    conn.commit()
    conn.close()

# ------------------------
# 3. Execute SQL by ID
# ------------------------
def execute_query_by_id(query_id):
    """
    Executes a SQL statement from SQLStore by its ID
    and logs result into SQLStoreStatus
    """
    conn = get_connection()
    if not conn:
        return {"error": "Database connection failed"}

    cursor = conn.cursor(dictionary=True)

    # 1. Fetch SQL statement
    cursor.execute("SELECT * FROM SQLStore WHERE id = %s", (query_id,))
    query_row = cursor.fetchone()
    if not query_row:
        conn.close()
        return {"error": f"No query found with id {query_id}"}

    sql = query_row["description"]

    # 2. Execute query
    try:
        cursor.execute(sql)
        if cursor.with_rows:
            result = cursor.fetchall()
        else:
            conn.commit()
            result = {"rows_affected": cursor.rowcount}

        # 3. Log success
        log_status(
            source_id=query_id,
            request=f"POST /start/{query_id}",
            status="Success",
            output=result
        )

        conn.close()
        return {
            "id": query_id,
            "status": "Success",
            "output": result
        }

    except Error as e:
        # Log failure
        log_status(
            source_id=query_id,
            request=f"POST /start/{query_id}",
            status="Failed",
            output=str(e)
        )
        conn.close()
        return {"id": query_id, "status": "Failed", "error": str(e)}

# ------------------------
# 4. Get latest execution status
# ------------------------
def get_last_status(query_id):
    """
    Fetch the latest execution status for a given SQLStore ID
    """
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

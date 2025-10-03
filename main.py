# from fastapi import FastAPI
# from db import execute_query_by_id, log_status, get_last_status

# app = FastAPI(title="SQL Execution API")

# # ------------------------
# # POST /start/{id} - execute a query
# # ------------------------
# @app.post("/start/{query_id}")
# def start_query(query_id: int):
#     """
#     Execute a query by its SQLStore ID and log the result
#     """
#     result = execute_query_by_id(query_id)
#     return result

# # ------------------------
# # POST /stop/{id} - stop a query
# # ------------------------
# @app.post("/stop/{query_id}")
# def stop_query(query_id: int):
#     """
#     Mark a query execution as stopped
#     """
#     # Log the stop action in SQLStoreStatus
#     log_status(
#         source_id=query_id,
#         request=f"POST /stop/{query_id}",
#         status="Stopped",
#         output="Query execution stopped by user"
#     )
#     return {
#         "id": query_id,
#         "status": "Stopped",
#         "message": "Query execution has been stopped"
#     }

# # ------------------------
# # GET /status/{id} - get last execution
# # ------------------------
# @app.get("/status/{query_id}")
# def status(query_id: int):
#     """
#     Get the latest execution status for a given SQLStore ID
#     """
#     row = get_last_status(query_id)
#     if not row:
#         return {"error": f"No execution found for query ID {query_id}"}
#     return row



from fastapi import FastAPI, Query
from db import (
    execute_query_by_id,
    log_status,
    get_last_status,
    get_all_query_ids,
    run_queries_multithreaded,
    run_queries_multiprocessed,
    run_queries_hybrid
)

app = FastAPI(title="SQL Execution API")

# ------------------------
# POST /start/{id} - execute a single query
# ------------------------
@app.post("/start/{query_id}")
def start_query(query_id: int):
    """
    Execute a query by its SQLStore ID and log the result
    """
    result = execute_query_by_id(query_id)
    return result

# ------------------------
# POST /stop/{id} - stop a query
# ------------------------
@app.post("/stop/{query_id}")
def stop_query(query_id: int):
    """
    Mark a query execution as stopped
    """
    log_status(
        source_id=query_id,
        request=f"POST /stop/{query_id}",
        status="Stopped",
        output="Query execution stopped by user"
    )
    return {"id": query_id, "status": "Stopped", "message": "Query execution has been stopped"}

# ------------------------
# GET /status/{id} - get last execution
# ------------------------
@app.get("/status/{query_id}")
def status(query_id: int):
    """
    Get the latest execution status for a given SQLStore ID
    """
    row = get_last_status(query_id)
    if not row:
        return {"error": f"No execution found for query ID {query_id}"}
    return row

# ------------------------
# POST /start_all - run all queries
# mode: "thread", "process", "hybrid"
# ------------------------
@app.post("/start_all")
def start_all(mode: str = Query("thread")):
    """
    Execute all queries from SQLStore in the selected mode:
    - thread: multithreading
    - process: multiprocessing
    - hybrid: multithreading + multiprocessing
    """
    query_ids = get_all_query_ids()
    if not query_ids:
        return {"error": "No queries found in SQLStore"}

    try:
        if mode == "thread":
            results = run_queries_multithreaded(query_ids)
        elif mode == "process":
            results = run_queries_multiprocessed(query_ids)
        elif mode == "hybrid":
            results = run_queries_hybrid(query_ids)
        else:
            return {"error": "Invalid mode. Choose 'thread', 'process', or 'hybrid'."}
    except Exception as e:
        return {"error": f"Execution failed: {str(e)}"}

    return {"status": "All queries executed", "mode": mode, "results_count": len(results)}

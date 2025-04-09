def generate_flow_payload(source_query, target_table, target_columns):
    return {
        "sql": source_query,
        "target_table": target_table,
        "target_columns": target_columns
    }
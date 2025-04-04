import re
from collections import defaultdict

def is_number(value):
    """Check if a value is a number (integer or float)."""
    return re.fullmatch(r"-?\d+(\.\d+)?", value) is not None

def generate_polars_sql(filter_str):
    queries = defaultdict(list)  # Stores WHERE conditions
    joins = defaultdict(set)  # Stores JOIN conditions
    referenced_tables = defaultdict(set)  # Tracks tables that are joined

    filters = filter_str.split(";")
    
    for f in filters:
        match = re.match(r"([\w]+)\.([\w]+)(=|<>)([\w\.]+)", f)
        if match:
            table, column, operator, values_str = match.groups()

            # Handle table-to-table reference (JOIN)
            if "." in values_str:
                ref_table, ref_column = values_str.split(".")
                joins[table].add(f"JOIN {ref_table} ON {table}.{column} {operator} {ref_table}.{ref_column}")
                referenced_tables[table].add(ref_table)  # Track referenced tables
            else:
                values = values_str.split("|")
                conditions = []
                
                for value in values:
                    if "*" in value:  # Wildcard handling for LIKE queries
                        conditions.append(f"{column} LIKE '{value.replace('*', '%')}'")
                    elif is_number(value):  # Keep numbers unquoted
                        conditions.append(f"{column} {operator} {value}")
                    else:  # Quote string values
                        conditions.append(f"{column} {operator} '{value}'")
                
                queries[table].append(f"({' OR '.join(conditions)})")
    
    # Construct SQL queries
    sql_queries = {}
    for table, conditions in queries.items():
        # Include JOINs if needed
        join_clause = " ".join(joins[table]) if table in joins else ""
        
        # If joins exist, select only table's columns explicitly
        select_clause = f"SELECT {table}.* FROM {table}" if table in referenced_tables else f"SELECT * FROM {table}"
        
        # Add WHERE clause if there are conditions
        where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql_queries[table] = f"{select_clause} {join_clause}{where_clause};"
    
    return sql_queries

# Example usage
filter_str = "dxTrade.sym=sert;dxTrade.price<>100;order.type<>limit|market;dxTrade.date=dxQuote.tradeDate"
sql_queries = generate_polars_sql(filter_str)
print(sql_queries)
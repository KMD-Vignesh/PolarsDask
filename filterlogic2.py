import re
from collections import defaultdict

def is_number(value):
    """Check if a value is a number (integer or float)."""
    return re.fullmatch(r"-?\d+(\.\d+)?", value) is not None

def generate_sql_queries(filter_str):
    queries = defaultdict(list)
    joins = defaultdict(set)  # To store join conditions

    filters = filter_str.split(";")
    
    for f in filters:
        match = re.match(r"([\w]+)\.([\w]+)(=|<>)([\w\.]+)", f)
        if match:
            table, column, operator, values_str = match.groups()

            # If the value refers to another table.column, it's a JOIN condition
            if "." in values_str:
                ref_table, ref_column = values_str.split(".")
                joins[table].add(f"JOIN {ref_table} ON {table}.{column} {operator} {ref_table}.{ref_column}")
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
    
    # Construct SQL queries with joins
    sql_queries = {}
    for table, conditions in queries.items():
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        join_clause = " ".join(joins[table]) if table in joins else ""
        sql_queries[table] = f"SELECT * FROM {table} {join_clause}{where_clause};"
    
    return sql_queries

# Example usage
filter_str = "dxTrade.sym=sert;dxTrade.price<>100;order.type<>limit|market;dxTrade.date=dxQuote.tradeDate"
sql_queries = generate_sql_queries(filter_str)
print(sql_queries)
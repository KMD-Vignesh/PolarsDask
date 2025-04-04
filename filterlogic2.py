import re
from collections import defaultdict

def is_number(value):
    """Check if a value is a number (integer or float)."""
    return re.fullmatch(r"-?\d+(\.\d+)?", value) is not None

def generate_sql_queries(filter_str):
    queries = defaultdict(list)
    filters = filter_str.split(";")
    
    for f in filters:
        match = re.match(r"([\w]+)\.([\w]+)(=|<>)(.+)", f)
        if match:
            table, column, operator, values_str = match.groups()
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
    
    # Convert conditions into full SQL queries
    sql_queries = {table: f"SELECT * FROM {table} WHERE {' AND '.join(conditions)};" 
                   for table, conditions in queries.items()}
    
    return sql_queries

# Example usage
filter_str = "dxTrade.sym=sert;dxTrade.price<>100;order.type<>limit|market;order.status=active;user.id=123;user.name=*row|admin*"
sql_queries = generate_sql_queries(filter_str)
print(sql_queries)
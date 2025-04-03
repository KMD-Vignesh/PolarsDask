import re
from collections import defaultdict

def generate_sql_queries(filter_str):
    queries = defaultdict(list)
    filters = filter_str.split(";")
    
    for f in filters:
        match = re.match(r"([\w]+)\.([\w]+)(=|<>)(.+)", f)
        if match:
            table, column, operator, values_str = match.groups()
            values = values_str.split("|")
            
            # Convert wildcard `*` to SQL LIKE pattern `%`
            if "*" in values_str:
                values = [f"'{v.replace('*', '%')}'" for v in values]
                condition = " OR ".join(f"{column} LIKE {v}" for v in values)
            else:
                values = [f"'{v}'" for v in values]
                condition = " OR ".join(f"{column} {operator} {v}" for v in values)

            queries[table].append(f"({condition})")
    
    # Convert conditions into full SQL queries
    sql_queries = {table: f"SELECT * FROM {table} WHERE {' AND '.join(conditions)};" 
                   for table, conditions in queries.items()}
    
    return sql_queries

# Example usage
filter_str = "dxTrade.sym=sert;dxTrade.price<>100;order.type<>limit|market;order.status=active;user.name=*row|admin*"
sql_queries = generate_sql_queries(filter_str)
print(sql_queries)
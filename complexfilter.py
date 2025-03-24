import re

def generate_sql_query(filter_string: str, table_name: str = "quote") -> str:
    """Generates a full SQL query with SELECT * FROM <table> WHERE <conditions>."""
    where_clause = generate_sql_where(filter_string)
    return f"SELECT * FROM {table_name} {where_clause}".strip()

def generate_sql_where(filter_string: str) -> str:
    """Generates a SQL WHERE clause from a filter string."""
    if not filter_string or filter_string.strip() == "":
        return ""
    
    conditions = [cond.strip() for cond in filter_string.split(",")]
    where_clauses = []
    
    for condition in conditions:
        if not condition:
            continue
            
        table_col, operator, values = parse_condition(condition)
        
        if "." not in table_col:
            raise ValueError(f"Invalid column format: {table_col}")

        table, column = table_col.split(".", 1)  # Handle `table.column`

        # Handle different operators
        if operator == "=":
            where_clauses.append(handle_equals(column, values))
        elif operator == "<>":
            where_clauses.append(handle_not_equals(column, values))
        elif operator == ">":
            where_clauses.append(f"{column} > {values}")
        elif operator == "<":
            where_clauses.append(f"{column} < {values}")

    return "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

def parse_condition(condition: str):
    """Parses a condition like 'quote.value=12' into (table.column, operator, values)."""
    operators = ["<>", "=", ">", "<"]
    for op in operators:
        if op in condition:
            table_col, values = condition.split(op, 1)
            return table_col.strip(), op, values.strip()
    raise ValueError(f"Invalid condition format: {condition}")

def handle_equals(column: str, values: str) -> str:
    """Handles equality conditions (=) with OR (`|`) and wildcards (`*`)."""
    if "|" in values:
        value_list = values.split("|")
        if any("*" in v for v in value_list):
            return "(" + " OR ".join(handle_sql_wildcard(column, v) for v in value_list) + ")"
        return f"{column} IN ({', '.join(format_value(v) for v in value_list)})"
    
    if "*" in values:
        return handle_sql_wildcard(column, values)
    
    return f"{column} = {format_value(values)}"

def handle_not_equals(column: str, values: str) -> str:
    """Handles NOT EQUAL (`<>`) conditions with OR (`|`) and wildcards (`*`)."""
    if "|" in values:
        return f"{column} NOT IN ({', '.join(format_value(v) for v in values.split('|'))})"
    
    if "*" in values:
        return f"NOT " + handle_sql_wildcard(column, values)

    return f"{column} <> {format_value(values)}"

def handle_sql_wildcard(column: str, value: str) -> str:
    """Handles SQL LIKE conditions for wildcard (`*`)."""
    like_pattern = value.replace("*", "%")
    return f"{column} LIKE '{like_pattern}'"

def format_value(value: str) -> str:
    """Formats values correctly: Numbers stay as is, strings are quoted."""
    return value if re.match(r"^\d+(\.\d+)?$", value) else f"'{value}'"

# Test Cases
filters = [
    "quote.value=12,quote.serial<>13",
    "quote.serial=16|17|18, quote.nam4l<>or|vup|sux",
    "quote.sourxe=bank|fran|cons",
    "quote.sourxe=*an*|*ns"
]

for i, filter_str in enumerate(filters, 1):
    sql = generate_sql_query(filter_str)
    print(f"SQL {i}: {sql}")
def generate_sql_where(filter_string: str) -> str:
    if not filter_string or filter_string.strip() == "":
        return ""
    
    conditions = [cond.strip() for cond in filter_string.split(",")]
    where_clauses = []
    
    for condition in conditions:
        if not condition:
            continue
            
        table_col, operator, values = parse_condition(condition)
        table, column = table_col.split(".")
        
        if operator == "=":
            if "|" in values:
                value_list = values.split("|")
                if any("*" in v for v in value_list):
                    wildcard_conditions = [handle_sql_wildcard(column, v) for v in value_list]
                    where_clauses.append("(" + " OR ".join(wildcard_conditions) + ")")
                else:
                    value_list = [f"'{v}'" if not v.isdigit() else v for v in value_list]
                    where_clauses.append(f"{column} IN ({', '.join(value_list)})")
            elif "*" in values:
                where_clauses.append(handle_sql_wildcard(column, values))
            else:
                value = values if values.isdigit() else f"'{values}'"
                where_clauses.append(f"{column} = {value}")
                
        elif operator == "<>":
            if "|" in values:
                value_list = values.split("|")
                value_list = [f"'{v}'" if not v.isdigit() else v for v in value_list]
                where_clauses.append(f"{column} NOT IN ({', '.join(value_list)})")
            else:
                value = values if values.isdigit() else f"'{values}'"
                where_clauses.append(f"{column} != {value}")
                
        elif operator == ">":
            where_clauses.append(f"{column} > {values}")
            
        elif operator == "<":
            where_clauses.append(f"{column} < {values}")
    
    if where_clauses:
        return "WHERE " + " AND ".join(where_clauses)
    return ""

def parse_condition(condition: str) -> tuple[str, str, str]:
    operators = ["<>", "=", ">", "<"]
    for op in operators:
        if op in condition:
            table_col, values = condition.split(op, 1)
            return table_col.strip(), op, values.strip()
    raise ValueError(f"Invalid condition format: {condition}")

def handle_sql_wildcard(column: str, value: str) -> str:
    if value.startswith("*") and value.endswith("*"):
        return f"{column} LIKE '%{value[1:-1]}%'"
    elif value.startswith("*"):
        return f"{column} LIKE '%{value[1:]}'"
    elif value.endswith("*"):
        return f"{column} LIKE '{value[:-1]}%'"
    else:
        return f"{column} = '{value}'"

if __name__ == "__main__":
    filter1 = "quote.value=12,quote.serial<>13"
    filter2 = "quote.serial=16|17|18, quote.nam4l<>or|vup|sux"
    filter3 = "quote.sourxe=bank|fran|cons"
    filter4 = "quote.sourxe=*an*|*ns"
    
    sql1 = generate_sql_where(filter1)
    sql2 = generate_sql_where(filter2)
    sql3 = generate_sql_where(filter3)
    sql4 = generate_sql_where(filter4)
    
    print("SQL 1:", sql1)
    print("SQL 2:", sql2)
    print("SQL 3:", sql3)
    print("SQL 4:", sql4)
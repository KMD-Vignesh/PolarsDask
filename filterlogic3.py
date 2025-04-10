import re
from collections import defaultdict
from typing import Any, Match

def is_number(value: str) -> bool:
    return re.fullmatch(r"\d+(\.\d+)?", value) is not None

def generate_sql_components(filter_str: str) -> dict:
    queries = defaultdict(list)
    joins = defaultdict(set)
    referenced_tables = defaultdict(set)

    filters = filter_str.split(";")  # Split by semicolon for AND conditions

    for f in filters:
        f = f.strip()
        if not f:
            continue

        match: Match[str] | None = re.match(r"([\w]+)\.([\w]+)(=|<>)([\w\.\*\|]+)", f)
        if not match:
            continue

        table, column, operator, values_str = match.groups()

        # If reference to another column (for JOIN)
        if "." in values_str and "*" not in values_str and "|" not in values_str:
            ref_table, ref_column = values_str.split(".")
            if ref_table != table:
                joins[table].add(f"JOIN {ref_table} ON {table}.{column} {operator} {ref_table}.{ref_column}")
                referenced_tables[table].add(ref_table)
            else:
                alias = f"{ref_table}_alias_{column}"
                joins[table].add(f"JOIN {ref_table} AS {alias} ON {table}.{column} {operator} {alias}.{ref_column}")
                referenced_tables[table].add(alias)
            continue

        values = values_str.split("|")
        conditions = []

        for value in values:
            value = value.strip()
            if "*" in value:
                like_val = value.replace("*", "%")
                conditions.append(f"{table}.{column} LIKE '{like_val}'")
            elif is_number(value):
                conditions.append(f"{table}.{column} {operator} {value}")
            else:
                conditions.append(f"{table}.{column} {operator} '{value}'")

        queries[table].append(f"({' OR '.join(conditions)})")

    return {
        "queries": queries,
        "joins": joins,
        "referenced_tables": referenced_tables
    }

def generate_sql_by_table(filter_str: str) -> dict:
    components = generate_sql_components(filter_str)
    sql_per_table = {}

    all_tables = set(components["queries"].keys()) | set(components["joins"].keys())

    for table in all_tables:
        joins = components["joins"].get(table, [])
        where_clauses = components["queries"].get(table, [])

        from_clause = f"FROM {table}"
        for join in joins:
            from_clause += f"\n  {join}"

        sql = f"SELECT *\n{from_clause}"
        if where_clauses:
            sql += "\nWHERE " + "\n  AND ".join(where_clauses)

        sql_per_table[table] = sql

    return sql_per_table
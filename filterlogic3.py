import re
from collections import defaultdict
from typing import Any, Match

def is_number(value: str) -> bool:
    return re.fullmatch(r"\d+(\.\d+)?", value) is not None

def generate_sql_queries(filter_str: str) -> dict:
    """
    Parses a filter string and generates SQL fragments with support for:
    - =, <>
    - OR logic using |
    - Wildcards using *
    - JOINs and self-JOINs (with aliasing)
    - AND conditions using semicolon (;)
    """
    queries = defaultdict(list)
    joins = defaultdict(set)
    referenced_tables = defaultdict(set)
    
    filters = filter_str.split(";")  # AND conditions split by ;

    for f in filters:
        match: Match[str] | None = re.match(r"([\w]+)\.([\w]+)(=|<>)([\w\.\*\|]+)", f.strip())
        if not match:
            continue

        table, column, operator, values_str = match.groups()

        # Handle JOIN if value is in table.column format and no wildcard/OR
        if "." in values_str and "*" not in values_str and "|" not in values_str:
            ref_table, ref_column = values_str.split(".")
            if ref_table != table:
                joins[table].add(f"JOIN {ref_table} ON {table}.{column} {operator} {ref_table}.{ref_column}")
                referenced_tables[table].add(ref_table)
            else:
                alias = f"{ref_table}_alias_{column}"
                joins[table].add(f"JOIN {ref_table} AS {alias} ON {table}.{column} {operator} {alias}.{ref_column}")
                referenced_tables[table].add(alias)
            continue  # Skip normal condition if it's a join

        # OR condition handling
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
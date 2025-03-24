import polars as pl
from typing import List, Dict

def apply_complex_filter(df: pl.DataFrame, filter_string: str) -> pl.DataFrame:
    if not filter_string or filter_string.strip() == "":
        return df
    
    conditions = [cond.strip() for cond in filter_string.split(",")]
    filter_expressions = []
    
    for condition in conditions:
        if not condition:
            continue
            
        table_col, operator, values = parse_condition(condition)
        table, column = table_col.split(".")
        
        # Get column dtype to determine how to handle the comparison
        col_dtype = df[column].dtype
        
        if operator == "=":
            if "|" in values:
                value_list = values.split("|")
                if col_dtype.is_numeric():
                    value_list = [float(v) if v != "" else None for v in value_list]
                expr = pl.col(column).is_in(value_list)
            elif "*" in values:
                expr = handle_wildcard(column, values)
            else:
                if col_dtype.is_numeric():
                    value = float(values) if values != "" else None
                else:
                    value = values
                expr = pl.col(column) == value
                
        elif operator == "<>":
            if "|" in values:
                value_list = values.split("|")
                if col_dtype.is_numeric():
                    value_list = [float(v) if v != "" else None for v in value_list]
                expr = ~pl.col(column).is_in(value_list)
            else:
                if col_dtype.is_numeric():
                    value = float(values) if values != "" else None
                else:
                    value = values
                expr = pl.col(column) != value
                
        elif operator == ">":
            expr = pl.col(column) > float(values)
            
        elif operator == "<":
            expr = pl.col(column) < float(values)
            
        filter_expressions.append(expr)
    
    return df.filter(pl.all_horizontal(filter_expressions))

def parse_condition(condition: str) -> tuple[str, str, str]:
    operators = ["<>", "=", ">", "<"]
    for op in operators:
        if op in condition:
            table_col, values = condition.split(op, 1)
            return table_col.strip(), op, values.strip()
    raise ValueError(f"Invalid condition format: {condition}")

def handle_wildcard(column: str, values: str) -> pl.Expr:
    if values.startswith("*") and values.endswith("*"):
        return pl.col(column).str.contains(values[1:-1])
    elif values.startswith("*"):
        return pl.col(column).str.ends_with(values[1:])
    elif values.endswith("*"):
        return pl.col(column).str.starts_with(values[:-1])
    else:
        return pl.col(column) == values

if __name__ == "__main__":
    df = pl.DataFrame({
        "value": [12, 13, 14, 15],
        "serial": [13, 14, 15, 16],
        "sourxe": ["bank", "fran", "cons", "other"],
        "nam4l": ["or", "vup", "sux", "test"]
    }, schema={"value": pl.Int64, "serial": pl.Int64, "sourxe": pl.Utf8, "nam4l": pl.Utf8})
    
    filter1 = "quote.value=12,quote.serial<>13"
    filter2 = "quote.serial=16|17|18, quote.nam4l<>or|vup|sux"
    filter3 = "quote.sourxe=bank|fran|cons"
    filter4 = "quote.sourxe=*an*"
    
    result1 = apply_complex_filter(df, filter1)
    result2 = apply_complex_filter(df, filter2)
    result3 = apply_complex_filter(df, filter3)
    result4 = apply_complex_filter(df, filter4)
    
    print("Filter 1 result:")
    print(result1)
    print("\nFilter 2 result:")
    print(result2)
    print("\nFilter 3 result:")
    print(result3)
    print("\nFilter 4 result:")
    print(result4)
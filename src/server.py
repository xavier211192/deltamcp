#!/usr/bin/env python3
import asyncio
from fastmcp import FastMCP
import polars as pl
import os
import json

# Load table configuration
def load_table_config():
    config_path = "config/tables.json"
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        # Fallback to default configuration if file doesn't exist
        return {
            "tables": {
                "product": {"path": "src/data/deltalake/product", "description": "Product catalog"},
                "customers": {"path": "src/data/deltalake/customers", "description": "Customer information"},
                "orders": {"path": "src/data/deltalake/orders", "description": "Order transactions"},
                "inventory": {"path": "src/data/deltalake/inventory", "description": "Stock levels"},
                "sales_metrics": {"path": "src/data/deltalake/sales_metrics", "description": "Sales data"}
            }
        }

# Create FastMCP server
mcp = FastMCP("DeltaMCP")
TABLE_CONFIG = load_table_config()

@mcp.tool()
def list_tables() -> str:
    """List all available Delta tables with their basic info."""
    try:
        tables = []
        
        for table_name, config in TABLE_CONFIG["tables"].items():
            table_path = config["path"]
            description = config.get("description", "")
            
            if os.path.exists(table_path):
                df = pl.read_delta(table_path)
                tables.append(f"- {table_name}: {len(df)} rows, {len(df.columns)} columns - {description}")
            else:
                tables.append(f"- {table_name}: NOT FOUND at {table_path}")
        
        return "Available Delta tables:\n" + "\n".join(tables)
    except Exception as e:
        return f"Error listing tables: {str(e)}"

@mcp.tool()
def inspect_table_schema(table_name: str) -> str:
    """Get detailed schema and partition information for a Delta table."""
    try:
        from deltalake import DeltaTable
        
        if table_name not in TABLE_CONFIG["tables"]:
            available_tables = ", ".join(TABLE_CONFIG["tables"].keys())
            return f"Table '{table_name}' not found in configuration. Available tables: {available_tables}"
        
        table_path = TABLE_CONFIG["tables"][table_name]["path"]
        if not os.path.exists(table_path):
            return f"Table '{table_name}' configured but not found at path: {table_path}"
        
        dt = DeltaTable(table_path)
        schema = dt.schema()
        metadata = dt.metadata()
        
        # Format schema information
        schema_info = []
        for field in schema.fields:
            nullable = " (nullable)" if field.nullable else " (not null)"
            schema_info.append(f"  {field.name}: {field.type}{nullable}")
        
        # Format partition information
        partition_info = "None"
        if metadata.partition_columns:
            partition_info = ", ".join(metadata.partition_columns)
        
        result = f"""Table: {table_name}
Schema:
{chr(10).join(schema_info)}

Partition Columns: {partition_info}
Table ID: {metadata.id}
Created: {metadata.created_time}"""
        
        if metadata.name:
            result += f"\nName: {metadata.name}"
        if metadata.description:
            result += f"\nDescription: {metadata.description}"
            
        return result
    except Exception as e:
        return f"Error inspecting table {table_name}: {str(e)}"

@mcp.tool()
def query_table_sample(table_name: str, limit: int = 5) -> str:
    """Get sample data from a specific Delta table."""
    try:
        if table_name not in TABLE_CONFIG["tables"]:
            available_tables = ", ".join(TABLE_CONFIG["tables"].keys())
            return f"Table '{table_name}' not found in configuration. Available tables: {available_tables}"
        
        table_path = TABLE_CONFIG["tables"][table_name]["path"]
        if not os.path.exists(table_path):
            return f"Table '{table_name}' configured but not found at path: {table_path}"
        
        df = pl.read_delta(table_path)
        sample_data = df.head(limit)
        
        return f"Sample data from '{table_name}' (showing {limit} of {len(df)} rows):\n{sample_data}"
    except Exception as e:
        return f"Error querying table {table_name}: {str(e)}"

if __name__ == "__main__":
    mcp.run()
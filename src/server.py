#!/usr/bin/env python3
import asyncio
from fastmcp import FastMCP

# Create FastMCP server
mcp = FastMCP("DeltaMCP")

@mcp.tool()
def hello_world(name: str = "World") -> str:
    """Say hello to someone."""
    return f"Hello, {name}!"

@mcp.tool()
def get_delta_info() -> str:
    """Get basic information about the delta table."""    
    try:
        import polars as pl
        from deltalake import DeltaTable
        import os
        
        table_path = "src/data/deltalake/product"
        if not os.path.exists(table_path):
            return f"Delta table not found at {table_path}"
            
        dt = DeltaTable(table_path)
        df = pl.read_delta(table_path)
        
        return f"Delta table has {len(df)} rows and {len(df.columns)} columns"
    except Exception as e:
        return f"Error reading delta table: {str(e)}"

if __name__ == "__main__":
    mcp.run()
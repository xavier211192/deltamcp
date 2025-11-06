#!/usr/bin/env python3
"""Tests for MCP server tools using FastMCP testing patterns."""

import pytest
import asyncio
import sys
import os

# Add src to path so we can import server modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from fastmcp import Client
from server import mcp

@pytest.fixture
def mcp_server():
    """Return the MCP server instance for testing."""
    return mcp

class TestMCPTools:
    """Test MCP tools using FastMCP Client."""
    
    @pytest.mark.asyncio
    async def test_list_tables(self, mcp_server):
        """Test list_tables tool."""
        async with Client(mcp_server) as client:
            result = await client.call_tool("list_tables", {})
            
            # Check that response contains expected tables
            assert "Available Delta tables:" in result.data
            assert "product:" in result.data
            assert "customers:" in result.data
            assert "orders:" in result.data
            assert "inventory:" in result.data
            assert "sales_metrics:" in result.data
            # Check descriptions are included
            assert "Product catalog" in result.data
    
    @pytest.mark.asyncio
    async def test_inspect_table_schema(self, mcp_server):
        """Test inspect_table_schema tool."""
        async with Client(mcp_server) as client:
            # Test with existing table
            result = await client.call_tool("inspect_table_schema", {"table_name": "product"})
            
            assert "Table: product" in result.data
            assert "Schema:" in result.data
            assert "product_code:" in result.data
            assert "color:" in result.data
            assert "size:" in result.data
            assert "Partition Columns:" in result.data
            assert "Table ID:" in result.data
            
            # Test with non-existent table
            result = await client.call_tool("inspect_table_schema", {"table_name": "nonexistent"})
            assert "not found in configuration" in result.data
            assert "Available tables:" in result.data
    
    @pytest.mark.asyncio
    async def test_query_table_sample(self, mcp_server):
        """Test query_table_sample tool."""
        async with Client(mcp_server) as client:
            # Test with existing table and custom limit
            result = await client.call_tool("query_table_sample", {
                "table_name": "customers", 
                "limit": 3
            })
            
            assert "Sample data from 'customers'" in result.data
            assert "showing 3 of" in result.data
            assert "customer_id" in result.data
            
            # Test with default limit  
            result = await client.call_tool("query_table_sample", {"table_name": "product"})
            assert "Sample data from 'product'" in result.data
            assert "showing 5 of 4 rows" in result.data or "showing 4 of 4 rows" in result.data
            
            # Test with non-existent table
            result = await client.call_tool("query_table_sample", {"table_name": "nonexistent"})
            assert "not found in configuration" in result.data
    
    @pytest.mark.asyncio
    async def test_all_tables_schema_inspection(self, mcp_server):
        """Test schema inspection for all tables."""
        tables = ["product", "customers", "orders", "inventory", "sales_metrics"]
        
        async with Client(mcp_server) as client:
            for table in tables:
                result = await client.call_tool("inspect_table_schema", {"table_name": table})
                
                # Basic checks for each table
                assert f"Table: {table}" in result.data
                assert "Schema:" in result.data
                assert "Table ID:" in result.data
                print(f"✓ Successfully inspected schema for {table}")


def run_sync_tests():
    """Run tests synchronously for manual testing."""
    print("Testing MCP Tools with FastMCP Client")
    print("=" * 50)
    
    async def run_all_tests():
        server = mcp
        
        async with Client(server) as client:
            # Test list_tables
            print("Testing list_tables...")
            result = await client.call_tool("list_tables", {})
            print(f"✓ list_tables():")
            print(result.data)
            print()
            
            # Test inspect_table_schema
            print("Testing inspect_table_schema...")
            tables = ["product", "customers", "orders"]
            for table in tables:
                result = await client.call_tool("inspect_table_schema", {"table_name": table})
                print(f"✓ inspect_table_schema('{table}'):")
                print(result.data)
                print("-" * 50)
            
            # Test query_table_sample  
            print("Testing query_table_sample...")
            for table in tables:
                result = await client.call_tool("query_table_sample", {
                    "table_name": table, 
                    "limit": 3
                })
                print(f"✓ query_table_sample('{table}', 3):")
                print(result.data)
                print("-" * 50)
            
            # Test error cases
            print("Testing error cases...")
            result = await client.call_tool("inspect_table_schema", {"table_name": "nonexistent"})
            print(f"✓ inspect_table_schema('nonexistent'): {result.data}")
            
            result = await client.call_tool("query_table_sample", {"table_name": "nonexistent"})
            print(f"✓ query_table_sample('nonexistent'): {result.data}")
    
    # Run the async tests
    asyncio.run(run_all_tests())
    print("\n" + "=" * 50)
    print("All tests completed successfully! ✓")


if __name__ == "__main__":
    # Run tests manually without pytest
    try:
        run_sync_tests()
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
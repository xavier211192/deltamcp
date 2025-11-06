#!/usr/bin/env python3
import polars as pl
from datetime import datetime, timedelta
import random
import uuid

def create_customers_table():
    """Create customers table with 50 sample records."""
    print("Creating customers table...")
    
    # Sample data
    first_names = ["John", "Jane", "Mike", "Sarah", "David", "Lisa", "Chris", "Amy", "Tom", "Emma",
                  "Alex", "Kate", "Ryan", "Mia", "Jake", "Zoe", "Luke", "Eva", "Noah", "Ava"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                 "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"]
    
    customers = []
    base_date = datetime.now() - timedelta(days=365)
    
    for i in range(50):
        customer_id = f"CUST{i+1:04d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{i+1}@email.com"
        signup_date = base_date + timedelta(days=random.randint(0, 365))
        status = random.choice(["active", "active", "active", "inactive"])  # 75% active
        
        customers.append({
            "customer_id": customer_id,
            "name": name,
            "email": email,
            "signup_date": signup_date.date(),
            "status": status
        })
    
    df = pl.DataFrame(customers)
    table_path = "src/data/deltalake/customers"
    df.write_delta(table_path, mode='overwrite')
    print(f"Created customers table with {len(df)} records at {table_path}")
    return df

def create_orders_table():
    """Create orders table with 200+ sample records."""
    print("Creating orders table...")
    
    # Get existing product codes
    products_df = pl.read_delta("src/data/deltalake/product")
    product_codes = products_df["product_code"].to_list()
    
    # Get existing customer IDs
    customers_df = pl.read_delta("src/data/deltalake/customers")
    customer_ids = customers_df["customer_id"].to_list()
    
    orders = []
    base_date = datetime.now() - timedelta(days=90)
    
    for i in range(250):  # 250 orders
        order_id = f"ORD{i+1:06d}"
        customer_id = random.choice(customer_ids)
        product_code = random.choice(product_codes)
        quantity = random.randint(1, 5)
        order_date = base_date + timedelta(days=random.randint(0, 90))
        
        # Simple pricing logic
        base_price = {"0001": 29.99, "0002": 34.99, "0003": 39.99, "0004": 44.99}
        total_amount = round(base_price.get(product_code, 35.00) * quantity, 2)
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_code": product_code,
            "quantity": quantity,
            "order_date": order_date.date(),
            "total_amount": total_amount
        })
    
    df = pl.DataFrame(orders)
    table_path = "src/data/deltalake/orders"
    df.write_delta(table_path, mode='overwrite')
    print(f"Created orders table with {len(df)} records at {table_path}")
    return df

def create_inventory_table():
    """Create inventory table for all products across warehouses."""
    print("Creating inventory table...")
    
    # Get existing product codes
    products_df = pl.read_delta("src/data/deltalake/product")
    product_codes = products_df["product_code"].to_list()
    
    warehouses = ["WH-EAST", "WH-WEST", "WH-CENTRAL"]
    inventory = []
    
    for product_code in product_codes:
        for warehouse in warehouses:
            stock_quantity = random.randint(10, 500)
            last_updated = datetime.now() - timedelta(days=random.randint(0, 7))
            
            inventory.append({
                "product_code": product_code,
                "warehouse_location": warehouse,
                "stock_quantity": stock_quantity,
                "last_updated": last_updated.date()
            })
    
    df = pl.DataFrame(inventory)
    table_path = "src/data/deltalake/inventory"
    df.write_delta(table_path, mode='overwrite')
    print(f"Created inventory table with {len(df)} records at {table_path}")
    return df

def create_sales_metrics_table():
    """Create sales metrics table with 30 days of data."""
    print("Creating sales_metrics table...")
    
    # Get existing product codes
    products_df = pl.read_delta("src/data/deltalake/product")
    product_codes = products_df["product_code"].to_list()
    
    regions = ["North", "South", "East", "West", "Central"]
    sales_metrics = []
    base_date = datetime.now() - timedelta(days=90)
    
    for day in range(30):  # Reduced from 90 to 30 days
        current_date = base_date + timedelta(days=day)
        
        for product_code in product_codes:
            for region in regions:
                # Some days might have no sales
                if random.random() < 0.5:  # Reduced from 70% to 50% chance of sales
                    sales_count = random.randint(1, 20)
                    base_price = {"0001": 29.99, "0002": 34.99, "0003": 39.99, "0004": 44.99}
                    revenue = round(base_price.get(product_code, 35.00) * sales_count, 2)
                    
                    sales_metrics.append({
                        "date": current_date.date(),
                        "product_code": product_code,
                        "sales_count": sales_count,
                        "revenue": revenue,
                        "region": region
                    })
    
    df = pl.DataFrame(sales_metrics)
    table_path = "src/data/deltalake/sales_metrics"
    df.write_delta(table_path, mode='overwrite')
    print(f"Created sales_metrics table with {len(df)} records at {table_path}")
    return df

def create_product_table():
    """Create product table with sample records."""
    print("Creating product table...")
    
    data = {
        'product_code': ['0001', '0002', '0003', '0004'],
        'color': ['red', 'green', 'blue', 'yellow'],
        'size': ['small', 'medium', 'large', 'x-large']
    }
    
    df = pl.DataFrame(data).with_columns([
        pl.lit(True).alias('is_current'),
    ])
    
    table_path = "src/data/deltalake/product"
    df.write_delta(table_path, mode='overwrite')
    print(f"Created product table with {len(df)} records at {table_path}")
    return df

def main():
    """Create all tables."""
    print("Setting up Delta Lake tables...")
    print("=" * 50)
    
    try:
        product_df = create_product_table()
        customers_df = create_customers_table()
        orders_df = create_orders_table()
        inventory_df = create_inventory_table()
        sales_metrics_df = create_sales_metrics_table()
        
        print("=" * 50)
        print("All tables created successfully!")
        print(f"- Product: {len(product_df)} records")
        print(f"- Customers: {len(customers_df)} records")
        print(f"- Orders: {len(orders_df)} records")
        print(f"- Inventory: {len(inventory_df)} records")
        print(f"- Sales Metrics: {len(sales_metrics_df)} records")
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

if __name__ == "__main__":
    main()
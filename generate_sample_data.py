#!/usr/bin/env python3
"""
AI-Powered Automated Data Pipeline - Sample Data Generator
Generate comprehensive sample datasets for testing and development.

This script creates realistic business data across 5 related tables:
- Customers (1,000 records)
- Products (200 records) 
- Employees (200 records)
- Orders (5,000 records)
- Transactions (5,000 records)

Total: 11,400 records with realistic business relationships
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set random seed for reproducible data
np.random.seed(42)
random.seed(42)

def create_sample_customers(n=1000):
    """Generate sample customer data with PII and business attributes."""
    logger.info(f"Generating {n} customer records...")
    
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", 
                   "William", "Jennifer", "James", "Mary", "Christopher", "Patricia", "Daniel"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez"]
    
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "business.org"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", 
              "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    
    customers = []
    for i in range(n):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        city_idx = random.randint(0, len(cities)-1)
        
        customer = {
            'customer_id': f"CUST_{i+1:06d}",
            'first_name': first_name,
            'last_name': last_name,
            'email': f"{first_name.lower()}.{last_name.lower()}{random.randint(1,999)}@{random.choice(domains)}",
            'phone': f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            'address': f"{random.randint(1,9999)} {random.choice(['Main', 'Oak', 'First', 'Second', 'Park', 'Elm'])} St",
            'city': cities[city_idx],
            'state': states[city_idx],
            'zip_code': f"{random.randint(10000,99999)}",
            'date_of_birth': (datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 365*3))).strftime('%Y-%m-%d'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic', 'VIP']),
            'credit_score': random.randint(300, 850),
            'annual_income': round(random.uniform(25000, 200000), 2),
            'is_active': random.choice([True, False]),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def create_sample_products(n=200):
    """Generate sample product catalog data."""
    logger.info(f"Generating {n} product records...")
    
    categories = ["Electronics", "Clothing", "Home & Garden", "Books", "Sports", "Toys", "Health"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "Generic"]
    
    products = []
    for i in range(n):
        category = random.choice(categories)
        product = {
            'product_id': f"PROD_{i+1:06d}",
            'product_name': f"{category} Item {i+1}",
            'category': category,
            'brand': random.choice(brands),
            'description': f"High-quality {category.lower()} product with excellent features",
            'price': round(random.uniform(9.99, 999.99), 2),
            'cost': round(random.uniform(5.0, 500.0), 2),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'dimensions': f"{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(5,50)} cm",
            'stock_quantity': random.randint(0, 1000),
            'reorder_level': random.randint(10, 100),
            'supplier_id': f"SUPP_{random.randint(1,50):03d}",
            'is_active': random.choice([True, False]),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        products.append(product)
    
    return pd.DataFrame(products)

def create_sample_employees(n=200):
    """Generate sample employee data with hierarchical relationships."""
    logger.info(f"Generating {n} employee records...")
    
    first_names = ["Alex", "Jordan", "Casey", "Taylor", "Morgan", "Jamie", "Drew", "Blake"]
    last_names = ["Anderson", "Thompson", "Wilson", "Moore", "Taylor", "Jackson", "White", "Harris"]
    departments = ["Sales", "Marketing", "IT", "HR", "Finance", "Operations", "Customer Service"]
    positions = ["Manager", "Senior Specialist", "Specialist", "Associate", "Coordinator"]
    
    employees = []
    for i in range(n):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        employee = {
            'employee_id': f"EMP_{i+1:06d}",
            'first_name': first_name,
            'last_name': last_name,
            'email': f"{first_name.lower()}.{last_name.lower()}@company.com",
            'phone': f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            'department': random.choice(departments),
            'position': random.choice(positions),
            'hire_date': (datetime.now() - timedelta(days=random.randint(30, 365*10))).strftime('%Y-%m-%d'),
            'salary': round(random.uniform(40000, 150000), 2),
            'manager_id': f"EMP_{random.randint(1,50):06d}" if i > 50 else None,
            'is_active': random.choice([True, False]),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        employees.append(employee)
    
    return pd.DataFrame(employees)

def create_sample_orders(customers_df, products_df, employees_df, n=5000):
    """Generate sample order data linking customers, products, and employees."""
    logger.info(f"Generating {n} order records...")
    
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    employee_ids = employees_df['employee_id'].tolist()
    
    order_statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
    
    orders = []
    for i in range(n):
        order_date = datetime.now() - timedelta(days=random.randint(1, 365*2))
        
        order = {
            'order_id': f"ORD_{i+1:08d}",
            'customer_id': random.choice(customer_ids),
            'product_id': random.choice(product_ids),
            'employee_id': random.choice(employee_ids),
            'order_date': order_date.strftime('%Y-%m-%d'),
            'ship_date': (order_date + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d'),
            'quantity': random.randint(1, 10),
            'unit_price': round(random.uniform(9.99, 999.99), 2),
            'discount_percent': round(random.uniform(0, 30), 1),
            'tax_amount': round(random.uniform(0, 100), 2),
            'total_amount': round(random.uniform(10, 1000), 2),
            'order_status': random.choice(order_statuses),
            'shipping_address': f"{random.randint(1,9999)} {random.choice(['Oak', 'Pine', 'Main'])} St",
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        orders.append(order)
    
    return pd.DataFrame(orders)

def create_sample_transactions(orders_df, n=5000):
    """Generate sample transaction data for financial processing."""
    logger.info(f"Generating {n} transaction records...")
    
    order_ids = orders_df['order_id'].tolist()
    transaction_types = ["Sale", "Refund", "Adjustment", "Fee", "Discount"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"]
    
    transactions = []
    for i in range(n):
        transaction_date = datetime.now() - timedelta(days=random.randint(1, 365*2))
        
        transaction = {
            'transaction_id': f"TXN_{i+1:08d}",
            'order_id': random.choice(order_ids) if random.random() > 0.1 else None,
            'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
            'transaction_type': random.choice(transaction_types),
            'amount': round(random.uniform(-1000, 1000), 2),
            'currency': 'USD',
            'payment_method': random.choice(payment_methods),
            'payment_reference': f"REF_{random.randint(100000, 999999)}",
            'merchant_id': f"MERCH_{random.randint(1,100):03d}",
            'processor_response': random.choice(['Approved', 'Declined', 'Pending']),
            'processor_fee': round(random.uniform(0, 10), 2),
            'is_processed': random.choice([True, False]),
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)

def main():
    """Main function to generate all sample datasets."""
    logger.info("Starting sample data generation...")
    
    # Create data directory
    data_dir = Path("data/sample")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate datasets
    customers_df = create_sample_customers(1000)
    products_df = create_sample_products(200)
    employees_df = create_sample_employees(200)
    orders_df = create_sample_orders(customers_df, products_df, employees_df, 5000)
    transactions_df = create_sample_transactions(orders_df, 5000)
    
    # Save to CSV files
    customers_df.to_csv(data_dir / "customers.csv", index=False)
    products_df.to_csv(data_dir / "products.csv", index=False)
    employees_df.to_csv(data_dir / "employees.csv", index=False)
    orders_df.to_csv(data_dir / "orders.csv", index=False)
    transactions_df.to_csv(data_dir / "transactions.csv", index=False)
    
    # Generate summary statistics
    total_records = len(customers_df) + len(products_df) + len(employees_df) + len(orders_df) + len(transactions_df)
    
    logger.info("Sample data generation completed!")
    logger.info(f"Generated {total_records:,} total records across 5 tables:")
    logger.info(f"  - Customers: {len(customers_df):,} records")
    logger.info(f"  - Products: {len(products_df):,} records")
    logger.info(f"  - Employees: {len(employees_df):,} records")
    logger.info(f"  - Orders: {len(orders_df):,} records")
    logger.info(f"  - Transactions: {len(transactions_df):,} records")
    logger.info(f"Data saved to: {data_dir.absolute()}")

if __name__ == "__main__":
    main()
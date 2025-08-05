import pandas as pd
import os

print('📊 Data Quality Report')
print('=' * 40)

if os.path.exists('data/sample'):
    files = ['customers.csv', 'products.csv', 'employees.csv', 'orders.csv', 'transactions.csv']
    for file in files:
        if os.path.exists(f'data/sample/{file}'):
            df = pd.read_csv(f'data/sample/{file}')
            print(f'✅ {file}: {len(df)} rows, {len(df.columns)} columns')
            print(f'   Memory usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB')
            print(f'   Sample: {list(df.columns[:3])}...')
        else:
            print(f'❌ {file}: Not found')
else:
    print('❌ data/sample directory not found')

"""
This is a basic Python program that manipulates and analyzes personal finance data in a CSV file. This was created with the intention of working with Python, Pandas, Numpy, and ETL Pipelines.

Note: This project was originally designed from a Claude prompt.
"""

import pandas as pd
import numpy as np
from datetime import datetime

def create_sample_data(filename = 'finance_data.csv', num_records = 1000):

    dates = pd.date_range(start = '2024-01-01', periods=num_records, freq = 'D')
    data = {
        'date': dates,
        'type': np.random.choice(['Deposit', 'Withdrawal', 'Purchase', 'Interest'], num_records),
        'amount': np.random.randint(1, 100, num_records),
    }


    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print("Sample data created in", filename)

def process_finance_data(input_file = "finance_data.csv", output_file = "processed_finance_data.csv"):

    print("Extracting data...")
    df = pd.read_csv(input_file)

    print("Transforming data...")

    df['date'] = pd.to_datetime(df['date'])

    df['month_year'] = df['date'].dt.strftime('%m-%Y')
    
    monthly_stats = df.groupby(['month_year', 'type']).agg({
        'amount': 'sum'
    }).reset_index()

    monthly_stats.columns = ["Month and Year", "Transaction Type", "Total Amount"]

    print("Loading processed data...")

    monthly_stats.to_csv(output_file, index=False)

    print("Processed data saved to", output_file)




if __name__ == "__main__":
    
    create_sample_data()

    process_finance_data()








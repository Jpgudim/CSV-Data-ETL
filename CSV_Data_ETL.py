"""
This is a basic Python program that manipulates and analyzes personal finance data in a CSV file. This was created with the intention of working with Python, Pandas, Numpy, and ETL Pipelines.

Note: This project was originally designed from a Claude prompt.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import mysql.connector

import os
from dotenv import load_dotenv

load_dotenv()

finance_db = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = os.getenv("mysql_password"),
    database = "finance_db"
)

mycursor = finance_db.cursor()

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

def process_monthly_totals(input_file = "finance_data.csv", output_file = "processed_monthly_totals.csv"):

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

    return monthly_stats

def process_average_by_transaction_type(input_file = "finance_data.csv", output_file = "processed_average_by_type.csv"):

    print("Extracting data...")
    df = pd.read_csv(input_file)

    print("Transforming data...")

    #df['date'] = pd.to_datetime(df['date'])

    #df['month_year'] = df['date'].dt.strftime('%m-%Y')
    
    transaction_average = df.groupby(['type']).agg({
        'amount': 'mean'
    }).reset_index()

    transaction_average.columns = ["Transaction Type", "Average Amount"]

    print("Loading processed data...")

    transaction_average.to_csv(output_file, index=False)

    print("Processed data saved to", output_file)

def process_start_end_bal (start_balance, input_file = "finance_data.csv", output_file = "processed_start_end_bal.csv"):
    print("Extracting data...")

    df = pd.read_csv(input_file)

    print("Transforming data...")

    balance = start_balance

    for index, row in df.iterrows():
        if row['type'] == "Deposit" or row['type'] == "Interest":
            balance += row['amount']
        elif row['type'] == "Withdrawal" or row ['type'] == "Purchase":
            balance -= row['amount']
    
    output_bal = {"Starting balance:": start_balance, "Ending balance:": balance}

    output_df = pd.DataFrame([output_bal])

    print("Loading processed data...")

    output_df.to_csv(output_file, index=False)

    print("Processed data saved to", output_file)

    print(output_df.to_string(index=False))



def add_data_to_db(input_file = "finance_data.csv"):
    df = pd.read_csv(input_file)

    for index, row in df.iterrows():
        sql = "INSERT INTO transactions (date, type, amount) VALUES (%s, %s, %s)"
        val = (row['date'], row['type'], row['amount'])
        mycursor.execute(sql, val)
        finance_db.commit()

    print("Data imported to databse")
             


if __name__ == "__main__":
    
    #create_sample_data()

    #process_monthly_totals()

    #process_average_by_transaction_type()

    #process_start_end_bal(0)

    add_data_to_db()
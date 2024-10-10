import pandas as pd
import sqlite3
import os

def init_db():
    conn = sqlite3.connect('production_data.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS annual_production (
        api_well_number TEXT PRIMARY KEY,
        oil REAL,
        gas REAL,
        brine REAL
    )
    ''')
    conn.commit()
    conn.close()

def insert_data(api_well_number, oil, gas, brine):
    conn = sqlite3.connect('production_data.db')
    cursor = conn.cursor()
    cursor.execute('''
    INSERT OR REPLACE INTO annual_production (api_well_number, oil, gas, brine)
    VALUES (?, ?, ?, ?)
    ''', (api_well_number, oil, gas, brine))
    conn.commit()
    conn.close()


def load_and_process_data():
    # Initialize the database
    init_db()

    # Load the data from excel sheet
    file_path = os.path.join('data', '20210309_2020_1.xls')
    df = pd.read_excel(file_path)

    print("Columns in Excel file:", df.columns)

    # Process and calculate annual data
    annual_data = df.groupby('API WELL  NUMBER').agg({
        'OIL': 'sum',
        'GAS': 'sum',
        'BRINE': 'sum'
    }).reset_index()

    # Insert data into the database
    for _, row in annual_data.iterrows():
        insert_data(row['API WELL  NUMBER'], row['OIL'], row['GAS'], row['BRINE'])

    print(f"Processed and inserted {len(annual_data)} records into the database.")


def get_well_data(api_well_number):
    conn = sqlite3.connect('production_data.db')
    cursor = conn.cursor()

    cursor.execute('''
    SELECT oil, gas, brine FROM annual_production
    WHERE api_well_number = ?
    ''', (api_well_number,))

    result = cursor.fetchone()
    conn.close()

    if result:
        return {
            "oil": result[0],
            "gas": result[1],
            "brine": result[2]
        }
    else:
        return None
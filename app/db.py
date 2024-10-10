import sqlite3

def init_db():
    connection = sqlite3.connect('annual_data.db')
    curs = connection.cursor()
    curs.execute('''
        CREATE TABLE IF NOT EXISTS Annual_Data (
            api_well_number TEXT PRIMARY KEY,
            oil INTEGER,
            gas INTEGER,
            brine INTEGER
        )
    ''')
    connection.commit()
    print(connection)
    connection.close()

def insert_data(api_well_number, oil, gas, brine):
    connect = sqlite3.connect('annual_data.db')
    cursor = connect.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO Annual_Data (api_well_number, oil, gas, brine)
        VALUES (?, ?, ?, ?)
    ''', (api_well_number, oil, gas, brine))
    connect.commit()
    connect.close()

def get_data(api_well_number):
    conn = sqlite3.connect('annual_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT oil, gas, brine FROM Annual_Data WHERE api_well_number = ?
    ''', (api_well_number, ))
    data = cursor.fetchone()
    conn.close()
    return data



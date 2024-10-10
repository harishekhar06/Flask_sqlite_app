import pandas as pd
from app.db import init_db, insert_data

def load_data():
    init_db()

    excel_file = 
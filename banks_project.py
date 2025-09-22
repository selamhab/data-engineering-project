import pandas as pd
from datetime import datetime
import sqlite3
from bs4 import BeautifulSoup
import requests


def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('table', class_='wikitable')
    print(f"Number of tables found: {len(tables)}")
    rows = tables[0].find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= len(table_attribs):
            data = [col.text.strip() for col in cols[:len(table_attribs)]]
            df.loc[len(df)] = data
    return df


def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path)
    rates = exchange_rate.set_index('Currency')['Rate'].to_dict()
    
    df['Market Cap (US$ Billion)'] = (
        df['Market Cap (US$ Billion)']
        .replace('[\$,]', '', regex=True)
        .astype(float)
    )
    if 'EUR' in rates:
        df['Market Cap (EUR)'] = (df['Market Cap (US$ Billion)'] * rates['EUR']).round(2)
    if 'GBP' in rates:
        df['Market Cap (GBP)'] = (df['Market Cap (US$ Billion)'] * rates['GBP']).round(2)
    if 'INR' in rates:
        df['Market Cap (INR)'] = (df['Market Cap (US$ Billion)'] * rates['INR']).round(2)

    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


def load_to_db(df, db_path, table_name):
    sql_connection = sqlite3.connect(db_path)
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    return sql_connection


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# === Main ETL Execution ===

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Rank", "Bank", "Market Cap"]
exchange_rate_csv = 'exchange_rate.csv'
output_csv = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
df = df.rename(columns={"Market Cap": "Market Cap (US$ Billion)"})

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchange_rate_csv)

df = df.rename(columns={
    "Market Cap (GBP)": "MC_GBP_Billion",
    "Market Cap (EUR)": "MC_EUR_Billion",
    "Market Cap (INR)": "MC_INR_Billion"
})

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_csv)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, db_name, table_name)
log_progress('Data loaded to Database as table. Running the query')

# Run each query correctly
run_query("SELECT * FROM Largest_banks", sql_connection)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
run_query("SELECT Bank from Largest_banks LIMIT 5", sql_connection)

log_progress('Process Complete.')
sql_connection.close()
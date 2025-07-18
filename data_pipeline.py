import os
import requests
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

headers = {
    'X-RapidAPI-Key': API_KEY,
}

url = "https://v3.football.api-sports.io/players/topscorers"
params = {"league":"39","season":"2023"}

def get_top_scorers(url, headers, params):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_error_message:
        print(f" [HTTP ERROR]: {http_error_message}")
        if response is not None:
            print(f"Response content: {response.text}")
        return None
    except requests.exceptions.ConnectionError as connection_error_message:
        print(f" [CONNECTION ERROR]: {connection_error_message}")
        return None
    except requests.exceptions.Timeout as timeout_error_message:
        print(f" [TIMEOUT ERROR]: {timeout_error_message}")
        return None
    except requests.exceptions.RequestException as other_error_message:
        print(f" [UNKNOWN ERROR]: {other_error_message}")
        return None

def process_top_scorers(data):
    top_scorers_list = []
    for scorer_data in data['response']:
        statistics = scorer_data['statistics'][0]
        player = scorer_data['player']
        
        player_name = player['name']
        club_name = statistics['team']['name']
        total_goals = int(statistics['goals']['total'])
        penalty_goals = int(statistics['penalty']['scored']) if statistics['penalty']['scored'] is not None else 0
        assists = int(statistics['goals']['assists']) if statistics['goals']['assists'] else 0
        matches_played = int(statistics['games']['appearences']) if statistics['games']['appearences'] is not None else 0
        minutes_played = int(statistics['games']['minutes']) if statistics['games']['minutes'] is not None else 0
        
        dob_str = player['birth'].get('date')
        if dob_str:
            dob = datetime.strptime(dob_str, '%Y-%m-%d')
            age = (datetime.now() - dob).days // 365
        else:
            age = None

        top_scorers_list.append({
            'player': player_name,
            'club': club_name,
            'total_goals': total_goals,
            'penalty_goals': penalty_goals,
            'assists': assists,
            'matches': matches_played,
            'mins': minutes_played,
            'age': age
        })
    return top_scorers_list

def create_dataframe(top_scorers_list):
    df = pd.DataFrame(top_scorers_list)
    df.sort_values(by=['total_goals', 'assists'], ascending=[False, False], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['position'] = df['total_goals'].rank(method='dense', ascending=False).astype(int)
    df = df[['position', 'player', 'club', 'total_goals', 'penalty_goals', 'assists', 'matches', 'mins', 'age']]
    return df

def create_db_connection(host_name, user_name, user_password, db_name):
    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful ✅")
    except Error as e:
        print(f"❌ [DATABASE CONNECTION ERROR]: '{e}'")
    return db_connection

def create_table(db_connection):
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS top_scorers (
        `position` INT,
        `player` VARCHAR(255),
        `club` VARCHAR(255),
        `total_goals` INT,
        `penalty_goals` INT,
        `assists` INT,
        `matches` INT,
        `mins` INT,
        `age` INT,
        PRIMARY KEY (`player`, `club`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Table created successfully ✅")
    except Error as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")
    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()

def insert_into_table(db_connection, df):
    cursor = db_connection.cursor()
    INSERT_DATA_SQL_QUERY = """
    INSERT INTO top_scorers (`position`, `player`, `club`, `total_goals`, `penalty_goals`, `assists`, `matches`, `mins`, `age`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `total_goals` = VALUES(`total_goals`),
        `penalty_goals` = VALUES(`penalty_goals`),
        `assists` = VALUES(`assists`),
        `matches` = VALUES(`matches`),
        `mins` = VALUES(`mins`),
        `age` = VALUES(`age`)
    """
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]
    try:
        cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
        db_connection.commit()
        print(f"Data inserted or updated successfully ✅. Rows affected: {cursor.rowcount}")
    except Error as e:
        print(f"❌ [INSERTING DATA ERROR]: '{e}'")
        db_connection.rollback()
    finally:
        cursor.close()

def run_data_pipeline():
    print("--- Starting ETL Pipeline ---")

    if not API_KEY:
        print("API_KEY is not set. Please update your .env file. ❌")
        return

    print("--- Starting Data Extraction ---")
    data = get_top_scorers(url, headers, params)

    if data and 'response' in data and data['response']:
        print("Data extraction successful. Proceeding to transformation.")
        top_scorers_list = process_top_scorers(data)
        df = create_dataframe(top_scorers_list)
        print("Data transformation successful.")
        print("\n--- Transformed Data Preview ---")
        print(df.to_string(index=False))
        print("-------------------------------\n")
    else:
        print("No data available from API or an error occurred during extraction ❌. Skipping transformation and loading.")
        return

    print("--- Starting Data Loading ---")
    db_connection = create_db_connection(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

    if db_connection:
        create_table(db_connection)
        insert_into_table(db_connection, df)
    else:
        print("Database connection failed, skipping data loading.")

    if db_connection and db_connection.is_connected():
        db_connection.close()
        print("Database connection closed.")
    print("--- Data Loading Finished ---")

    print("\n--- ETL Pipeline Finished ---")

if __name__ == "__main__":
    run_data_pipeline()
# Import modules
import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import datetime
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Initialize environmental variables from .env file
load_dotenv()

# Load client credentials from .env
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


# EXTRACT
def get_recently_played_songs():
    """
    This function authenticates with the Spotipy API using the provided client credentials.
    It then requests the user's recently played songs within the last 24 hours of the
    current date and time of execution.

    Spotipy library provides a more user-friendly interaction with the Spotify Web API.
    
    Parameters:
        - auth_manager (spotipy.oauth2.SpotifyOAuth):
            Handles the OAuth 2.0 authorization code flow to obtain the user's authorization tokens.
        - client_id (str):
            Identifies the application making requests to the Spotify API.
        - client_secret (str):
            Serves as a confidential identifier between the application and the Spotify API.
        - redirect_uri (str):
            The URI where the user will be redirected after granting or denying access.
            This URI must be registered on the Spotify Developer Dashboard.
        - scope (str or list of str):
            Defines the access requested by the application.
            For this function, the scope is set to "user-read-recently-played",
            allowing access to the user's recently played songs.

    Returns the response of the query as a Pandas DataFrame
    """
    # Use spotipy module for authentication code flow
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri="http://127.0.0.1:5000/redirect",
            scope="user-read-recently-played",
        )
    )

    # Create timestamp for 24 hours ago from the current datetime for query
    today = datetime.datetime.now()
    today_formatted = today.strftime("%m-%d-%y")
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    try:
        # Send api request using spotipy library to get played songs last 24 hours
        recently_played = sp.current_user_recently_played(
            after=yesterday_unix_timestamp
        )
        response = recently_played

        # Convert response to JSON string for data backup
        history = json.dumps(response)

        # Create lists for DataFrame columns
        song_names = []
        artist_names = []
        played_at_list = []
        timestamps = []

        # Extract relevant info from response JSON
        for song in response["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])

        song_dict = {
            "song_name": song_names,
            "artist_name": artist_names,
            "played_at": played_at_list,
            "timestamps": timestamps,
        }

        # Create DataFrame with extracted songs
        song_df = pd.DataFrame(song_dict)

        # Save response history for data backup
        history_file_path = f"backup/history_{today_formatted}.json"
        with open(history_file_path, "w") as outfile:
            outfile.write(history)

        # Save DataFrame as csv file for sample reference
        song_df.to_csv("Daily_sample.csv")
        print("Extract Successful\n" "Songs played recently:")
        print(song_df)
        return song_df

    except Exception as e:
        raise Exception(f"Error: {e}")


# Validate
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        raise False
    # Primary Key Check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")


    # EXTRACT data: get user recently played songs
    song_df = get_recently_played_songs()

        
def load_db(song_df):
    # LOAD
    try:
        # Define database connection parameters
        user = "postgres"
        host = "localhost"
        port = "5432"
        password = "root"
        dbname = "Spotify_ETL"
        conn_string = f"dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'"

        # Create connection with Postgres DB
        conn = psycopg2.connect(conn_string)

    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")

    # Create engine for communicating with database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_play_history(
            song_name TEXT,
            artist_name TEXT,
            played_at TEXT,
            timestamps TEXT,
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """
    cursor.execute(sql_query)
    conn.commit()
    print("Connected to database ")

    # Upload DataFrame to database
    try:
        song_df.to_sql("my_play_history", engine, index=False, if_exists="append")
        print("Upload successful")
    except Exception as e:
        print(f"Upload error: {e}")

def main():
    # Get recently played songs data
    song_df = get_recently_played_songs()

    # Check if data obtained is valid
    if check_if_valid_data(song_df):
        print("Data valid, proceed to load")

    # Load valid data to database
    load_db(song_df=song_df)

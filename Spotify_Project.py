#region Imports
from io import StringIO
from pandas.io.formats.format import Datetime64Formatter
import requests
from requests.api import head
import sqlalchemy
from sqlalchemy import insert
import json
from datetime import date, datetime, time
import datetime as dt
import pandas as pd
import psycopg2
import calendar
from pytz import timezone
from dateutil import tz
from tzlocal import get_localzone
from secrets import SPOTIFY_USER_ID
from refresh import Refresh
from_zone = tz.tzutc()
to_zone = tz.tzlocal()
#endregion

class SaveSongs:
    def _init_(self):
        self.spotify_token = ""

    # Validate data by checking for blanks or null values
    def validate_data(self, df: pd.DataFrame) -> bool:
        if df.empty:
            #print("No songs listened to recently")
            raise Exception("No songs listened to recently?")

        if df.isnull().values.any():
            raise Exception("Null values exist")

        return True

    def get_recent_songs(self):

        # Get datetime for
        current_datetime = dt.datetime.now()
        current_datetime_minus_24hr = current_datetime - dt.timedelta(days=1)
        current_datetime_minus_24hr_unix_timestamp = int(current_datetime_minus_24hr.timestamp()) * 1000

        r =requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}&limit={limit}".format(time=current_datetime_minus_24hr_unix_timestamp, limit=50), 
                        headers = {"Accept" : "application/json",
                                "Content-Type" : "application/json",
                                "Authorization" : "Bearer {token}".format(token=self.spotify_token)})

        data = r.json()

        print(data)

        # Create lists to be populated by song data
        song_names = []
        artist_names = []
        album_names = []
        played_at_list = []
        played_date = []
        play_time_12_hour = []
        played_at_local_time=[]
        played_at_day=[]
        song_ids = []
        song_ids_string = ""
        played_at_full_string = ""

        count = 0
        
        # Loop through each song and grab data
        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            album_names.append(song["track"]["album"]["name"])
            
            if (song["track"]["id"] not in song_ids and song["track"]["id"] is not None):
                song_ids_string += song["track"]["id"] + ","

            song_ids.append(song["track"]["id"])
            #print(song["track"]["id"])

            played_at_list.append(song["played_at"])
            #played_date.append(song["played_at"][0:10])

            played_at_datetime_utc = dt.datetime.strptime(played_at_list[count], "%Y-%m-%dT%H:%M:%S.%fZ")
            utc = played_at_datetime_utc.replace(tzinfo=from_zone)

            # Convert time zone
            if (utc.astimezone(to_zone) not in played_at_local_time and utc.astimezone(to_zone) is not None):
                played_at_full_string += str(utc.astimezone(to_zone)) + ","

            played_at_local_time.append(utc.astimezone(to_zone))
            played_date.append(str(utc.astimezone(to_zone))[0:10])
            time = (utc.astimezone(to_zone))

            # Add 12 hour time, Ex: 12:57 PM
            play_time_12_hour.append(time.strftime("%I:%M %p"))

            played_at_day.append(calendar.day_name[played_at_local_time[count].weekday()])

            count+=1
        
        # Create song details dictionary
        song_details_dict = {
            "song_id" : song_ids,
            "song_name" : song_names,
            "artist_name" : artist_names,
            "album_name" : album_names
        }

        # Create play history dictionary
        play_history_dict = {
            "song_id" : song_ids,
            "played_date" : played_date,
            "played_day" : played_at_day,
            "played_time" : play_time_12_hour,
            "played_time_full" : played_at_local_time
        }

        # Convert dictionaries into DataFrames
        song_details_df = pd.DataFrame(song_details_dict, columns=["song_id", "song_name", "artist_name", "album_name"])
        played_history_df = pd.DataFrame(play_history_dict, columns=["song_id", "played_date", "played_day", "played_time", "played_time_full"])


        # Validate data and pass to loading function
        if self.validate_data(played_history_df):
            # Load to Database
            self.load_data(played_history_df, "play_history", song_ids_string, played_at_full_string[:-1])
        
        if self.validate_data(song_details_df):
            # Load to Database
            self.load_data(song_details_df, "song_details", song_ids_string)

        # Remove last comma from song_ids_string
        return song_ids_string[:-1]


    # Retrieve audio features from API for multiple songs
    def get_features(self, song_ids_string):

        r =requests.get("https://api.spotify.com/v1/audio-features?ids={ids}".format(ids=song_ids_string), 
                        headers = {"Accept" : "application/json",
                                "Content-Type" : "application/json",
                                "Authorization" : "Bearer {token}".format(token=self.spotify_token)})

        danceability = []
        energy = []
        loudness = []
        speechiness = []
        acousticness = []
        instrumentalness = []
        liveness = []
        valence = []
        tempo = []
        duration_ms = []
        song_ids = []

        data = r.json()

        for song in data["audio_features"]:
            
            song_ids.append(song["id"])
            danceability.append(song["danceability"])
            energy.append(song["energy"])
            loudness.append(song["loudness"])
            speechiness.append(song["speechiness"])
            acousticness.append(song["acousticness"])
            instrumentalness.append(song["instrumentalness"])
            liveness.append(song["liveness"])
            valence.append(song["valence"])
            tempo.append(song["tempo"])
            duration_ms.append(song["duration_ms"])

        audio_features_dict = {
            "song_id" : song_ids,
            "danceability" : danceability,
            "energy" : energy,
            "loudness" : loudness,
            "speechiness" : speechiness,
            "acousticness" : acousticness,
            "instrumentalness" : instrumentalness,
            "liveness" : liveness,
            "valence" : valence,
            "tempo" : tempo,
            "duration_ms" : duration_ms

        }

        audio_features_df = pd.DataFrame(audio_features_dict, columns=["song_id", "danceability", "energy", "loudness", "speechiness", "acousticness",
        "instrumentalness", "liveness", "valence", "tempo", "duration_ms"])

        print(audio_features_df)

        self.load_data(audio_features_df, 'audio_features', song_ids_string)

    # Loading data into PostgreSQL
    def load_data(self, df: pd.DataFrame, table_name, song_ids, played_at_full_string = ""):
        engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost:5432/spotify_data")

        # Connect to Database in PostgreSQL
        conn = psycopg2.connect("dbname=spotify_data user=postgres password=postgres")
        cursor = conn.cursor()

        # If populating the audio features table, don't add songs that already exist
        if table_name == "audio_features":
            cursor.execute("SELECT COUNT(1) FROM information_schema.tables WHERE table_name = 'audio_features'")

            # Append data to table if it already exists
            if (cursor.fetchone()[0]==1):
                
                # Check if song ids already exist in the database
                song_ids = "'" + song_ids.replace(",", "', '") + "'"
                cursor.execute("SELECT song_id FROM audio_features WHERE song_id IN (" + song_ids + ")")
                existing_song_ids = cursor.fetchall()
                existing_ids_list = [item for id in existing_song_ids for item in id]
                df_with_index = df.set_index("song_id")
                df_without_duplicates = df_with_index.drop(existing_ids_list)
                self.copy_from_stringio(conn, df_without_duplicates, table_name)
                # The above list gives us all the records that already exist, so we want to remove those from our dataframe

                # Create table if it doesn't exist and add data
            else:
                df.drop_duplicates().to_sql(table_name, engine, index=False, if_exists = "append", chunksize = 1000)
        
        # If populating the song_details table, don't add songs that already exist
        elif table_name == "song_details":
            cursor.execute("SELECT COUNT(1) FROM information_schema.tables WHERE table_name = 'song_details'")

            # Check if table exists
            if (cursor.fetchone()[0]==1):
                # Check if song ids already exist in the database
                song_ids = "'" + song_ids.replace(",", "', '") + "'"
                cursor.execute("SELECT song_id FROM song_details WHERE song_id IN (" + song_ids + ")")
                existing_song_ids = cursor.fetchall()
                existing_ids_list = [item for id in existing_song_ids for item in id]
                df_with_index = df.set_index("song_id")
                df_without_duplicates = df_with_index.drop(existing_ids_list)
                # The above list gives us all the records that already exist, so we want to remove those from our dataframe

                # Get rid of duplicates in dataframe (occurs from same song played on same day)
                df_without_duplicates2 = df_without_duplicates.drop_duplicates()     
    

                self.copy_from_stringio(conn, df_without_duplicates2, table_name)

            # Create table if it doesn't exist and add data
            else:
                df.drop_duplicates().to_sql(table_name, engine, index=False, if_exists = "append", chunksize = 1000)
        
        # Load play_history as is, with duplicates valid
        elif table_name == "play_history":
            cursor.execute("SELECT COUNT(1) FROM information_schema.tables WHERE table_name = 'play_history'")

            # Append to table if it already exists
            if (cursor.fetchone()[0]==1):
                # Check if played_at_local already exist in the database
                played_time_full = "'" + played_at_full_string.replace(",", "', '") + "'"
                cursor.execute("SELECT played_time_full FROM play_history WHERE played_time_full IN (" + played_time_full + ")")
                existing_played_time = cursor.fetchall()
                existing_time_list = [item for time in existing_played_time for item in time]
                df_with_index = df.set_index("played_time_full", drop=False)
                df_without_duplicates = df_with_index.drop(existing_time_list)
                final_df = df_without_duplicates.set_index("song_id")
                # The above list gives us all the records that already exist, so we want to remove those from our dataframe

                self.copy_from_stringio(conn, final_df, table_name)

            # Create table if it doesn't exist and add data
            else:
                df.drop_duplicates().to_sql(table_name, engine, index=False, if_exists = "append", chunksize = 1000)

        cursor.close()
        conn.close()


    # Append data to existing tables
    def copy_from_stringio(self, conn, df, table):
        """
        Here we are going save the dataframe in memory 
        and use copy_from() to copy it to the table
        """
        # Save DF to memory buffer
        buffer = StringIO()
        df.to_csv(buffer, index_label='id', header=False, sep="\t")
        buffer.seek(0)
        
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep="\t")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("copy_from_stringio() done")
        cursor.close()    

    # Refresh token
    def call_refresh(self):
        refreshCaller = Refresh()
        self.spotify_token = refreshCaller.refresh()
        song_ids_string = self.get_recent_songs()
        self.get_features(song_ids_string)


a = SaveSongs()
a.call_refresh()

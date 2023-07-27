# Spotify-Data-Pipeline
An ETL pipeline that extracts daily played tracks from Spotify API

# ETL 
![ETL](Spotify_ETL.drawio.png)
<br><br> Extracts tracks played in the last 24 hours from Spotify API then transformed and validated using Python before uploaded to a local Postgres Database. Orchestrated using Airflow.

## Note
+ Makes use of spotipy module to handle authentication code flow for Spotify API
+ Registration to Spotify Dashboard is required to get ClientID and ClientSecret


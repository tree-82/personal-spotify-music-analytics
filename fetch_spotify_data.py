import pandas as pd
import requests
import base64
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Function to authenticate and get the access token
def get_spotify_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_header = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    headers = {'Authorization': f'Basic {auth_header}'}
    data = {'grant_type': 'client_credentials'}

    response = requests.post(auth_url, headers=headers, data=data)
    response_data = response.json()
    return response_data['access_token']

# Function to get track details from Spotify API, including audio features
def get_track_info(track_id, token, client_id, client_secret):
    url = f'https://api.spotify.com/v1/tracks/{track_id}'
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers)

        # Check if the token has expired
        if response.status_code == 401:
            print("Token expired. Fetching a new one...")
            token = get_spotify_token(client_id, client_secret)
            headers = {'Authorization': f'Bearer {token}'}
            response = requests.get(url, headers=headers)

        # Check for rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5)) 
            print(f"Rate limit reached. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return get_track_info(track_id, token, client_id, client_secret)

        response.raise_for_status() 
        track_data = response.json()
        artist_id = track_data['artists'][0]['id']
        release_date = track_data['album']['release_date']
        popularity = track_data.get('popularity')  # Popularity field

        # Extract the album image URL (select the largest available image)
        album_images = track_data['album']['images']
        album_image_url = album_images[0]['url'] if album_images else None

        # Call the artist API to get genres
        artist_url = f'https://api.spotify.com/v1/artists/{artist_id}'
        artist_response = requests.get(artist_url, headers=headers)
        artist_response.raise_for_status()

        artist_data = artist_response.json()
        genres = ', '.join(artist_data.get('genres', []))

        # Get audio features
        audio_features_url = f'https://api.spotify.com/v1/audio-features/{track_id}'
        audio_features_response = requests.get(audio_features_url, headers=headers)
        audio_features_response.raise_for_status()
        audio_features_data = audio_features_response.json()

        # Extract key features
        key = audio_features_data.get('key')
        tempo = audio_features_data.get('tempo')
        danceability = audio_features_data.get('danceability')
        energy = audio_features_data.get('energy')
        mode = audio_features_data.get('mode')  # 0 = minor, 1 = major
        loudness = audio_features_data.get('loudness')
        instrumentalness = audio_features_data.get('instrumentalness')
        speechiness = audio_features_data.get('speechiness')

        return {
            'release_date': release_date,
            'popularity': popularity,
            'genres': genres,
            'album_image_url': album_image_url,
            'key': key,
            'tempo': tempo,
            'danceability': danceability,
            'energy': energy,
            'mode': mode,
            'loudness': loudness,
            'instrumentalness': instrumentalness,
            'speechiness': speechiness,
            'token': token
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching track info for ID {track_id}: {e}")
        return None

def process_spotify_data(csv_file, client_id, client_secret):
    df = pd.read_csv(csv_file)

    # Extract track ID from the format 'spotify:track:{id}'
    df['TrackID'] = df['Spotify Track Url'].str.split(':').str[-1]

    if 'Processed' not in df.columns:
        df['Processed'] = 'No'
    token = get_spotify_token(client_id, client_secret)

    # Iterate over each row to get Spotify data
    for index, row in df.iterrows():
        track_id = row['TrackID']

        # Skip tracks that have already been processed
        if row['Processed'] == 'Yes':
            print(f"Track {track_id} has already been processed. Skipping...")
            continue

        track_info = get_track_info(track_id, token, client_id, client_secret)

        if track_info:
            # Update the row with the fetched Spotify data
            df.at[index, 'Genre'] = track_info['genres']
            df.at[index, 'ReleaseDate'] = track_info['release_date']
            df.at[index, 'Popularity'] = track_info['popularity']
            df.at[index, 'AlbumImageURL'] = track_info['album_image_url']
            df.at[index, 'Key'] = track_info['key']
            df.at[index, 'Tempo'] = track_info['tempo']
            df.at[index, 'Danceability'] = track_info['danceability']
            df.at[index, 'Energy'] = track_info['energy']
            df.at[index, 'Mode'] = track_info['mode']
            df.at[index, 'Loudness'] = track_info['loudness']
            df.at[index, 'Instrumentalness'] = track_info['instrumentalness']
            df.at[index, 'Speechiness'] = track_info['speechiness']
            df.at[index, 'Processed'] = 'Yes'  # Mark as processed

            # Save the updated DataFrame back to the original CSV
            df.to_csv(csv_file, index=False)

        # To avoid hitting API rate limits, add a slight delay
        time.sleep(2)

    print('Spotify data processing complete!')

# Example usage
csv_file = 'spotifypersonal.csv'
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

process_spotify_data(csv_file, client_id, client_secret)

import requests
import json 
from dotenv import load_dotenv
import os 

load_dotenv(dotenv_path='./.env')
api_key = os.getenv('api_key')

channelHandle = 'MrBeast'


def getPlaylistId(): 
    try : 
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channelHandle}&key={api_key}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        channelItems = data['items'][0]
        channelPlaylistId = channelItems["contentDetails"]["relatedPlaylists"]['uploads']
        
        return channelPlaylistId
    except requests.exceptions.RequestException as e :
        raise e 
 

if __name__ == '__main__' : 
    playlistId = getPlaylistId()
    print(playlistId)
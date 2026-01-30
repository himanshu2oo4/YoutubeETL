import requests
import json 
from dotenv import load_dotenv
import os 
from datetime import date 
load_dotenv(dotenv_path='./.env')
api_key = os.getenv('api_key')

channelHandle = 'MrBeast'

maxResults = 50

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
 

# def getVideosId(rec): 
#     try : 
#         playlistId = getPlaylistId()
#         url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={rec}&playlistId={playlistId}&key={api_key}'
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
#         for i in range(rec): 
#             video_id = data["items"][i]["contentDetails"]['videoId']
#             print(video_id)
#     except requests.exceptions.RequestException as e : 
#         raise e  
    

def getVideosId(playlistId) :
    '''
    takes a playlist : which means the channel id 
    goes to the content details and get the videos ID one by one 

    At one time you can access only 50 videos data at a single time 
    then you have to move on to next next pages by using the argument : PageToken 

    after gettting the data of first page then it move on to the next page by using the 

    ARG : nextPageToken : to move on to the next page and fetch next 50 videos data 

    '''



    video_ids= []
    pageToken = None 
    maxResults = 50
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={api_key}'
    try : 
        while len(video_ids)<200 :
            url = base_url 
            if pageToken : 
                url+= f'&pageToken={pageToken}'
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()
            for item in data.get('items', []): 
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            

            pageToken = data.get('nextPageToken')

            if not pageToken : 
                break 

        return video_ids
    
    except requests.exceptions.RequestException as e : 
        raise e 



def batch_list(video_id_lst , batch_size): 
    if batch_size<0 : 
        raise ValueError('batch size must be greater than 0')
    for video_id in range(0 , len(video_id_lst) , batch_size) : 
        yield video_id_lst[video_id:video_id+batch_size]




def extractVideoData(video_ids ): 
    extractedData = []
    try : 
        def batch_list(video_id_lst , batch_size): 
            if batch_size<0 : 
                raise ValueError('batch size must be greater than 0')
            for video_id in range(0 , len(video_id_lst) , batch_size) : 
                yield video_id_lst[video_id:video_id+batch_size]

        
        for batch in batch_list(video_ids , maxResults): 
            videoIdsStr = ','.join(batch)
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=statistics&part=snippet&id={videoIdsStr}&key={api_key}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get('items' , []):
                video_id = item['id'] 
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
            
                video_data = {

                    "video_id" : video_id, 
                    "title" : snippet['title'], 
                    "publishedAt" : snippet['publishedAt'], 
                    "duration": contentDetails['duration'], 
                    "viewCount" : statistics.get('viewCount' , None), 
                    "likeCount": statistics.get('likeCount' , None), 
                    "commentCount": statistics.get('commentCount' , None)

                }


                extractedData.append(video_data)
        return extractedData
    except requests.exceptions.RequestException  as e : 
        raise e 


def saveToJson(extractedData) : 
    path = f'./data/{channelHandle}_etl_data{date.today()}.json'
    # encoding: utf-8 to accept the special characters also 
    with open(path , 'w' , encoding= 'utf-8') as f : 
        json.dump(extractedData , f , indent=4 , ensure_ascii = False )


if __name__ == '__main__' : 
    playlistId = getPlaylistId()
    videoIds = getVideosId(playlistId)
    video_data  = extractVideoData(videoIds)
    saveToJson(video_data)


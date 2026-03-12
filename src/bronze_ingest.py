import requests 
import json
import boto3
from config import BUCKET,YT_API_KEY
from logger import get_logger

log=get_logger("bronze")

SEARCH_URL="https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL="https://www.googleapis.com/youtube/v3/videos"

YEAR= range(2015,2020)
TARGET_RESULTS=200

def search_videos(year:int ,target_count:int):
    all_items=[]
    next_page_token=None

    while len(all_items)<target_count:
        params={
            "part":"snippet",
            "type":"video",
            "videoCategoryId":"10",
            "order":"viewCount",
            "publishedAfter":f"{year}-01-01T00:00:00Z",
            "publishedBefore":f"{year}-12-31T23:59:59Z",
            "maxResults":50,
            "key":YT_API_KEY,

        }
        if next_page_token:
            params["pageToken"]=next_page_token
        r=requests.get(SEARCH_URL, params=params ,timeout=30)
        r.raise_for_status()
        data=r.json()

        items=data.get("items",[])
        all_items.extend(items)
        
        next_page_token=data.get("nextPageToken")

        log.info(f"{year}  search:{len(all_items)}")
        
        if not next_page_token:
            break
    return all_items[:target_count]

def get_statistics(video_ids :list):
    stats={}
    for i in range (0,len(video_ids),50):
        batch=video_ids[i:i+50]
        params={
            "part":"snippet,statistics",
            "id":",".join(batch),
            "key":YT_API_KEY,
        }
        try:
                r = requests.get(VIDEOS_URL, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()

                for item in data.get("items", []):
                    vid = item["id"]
                   
                    stats[vid] = item 
        except Exception as e:
                print(f"Error obteniendo stats para un batch: {e}")
            
    return stats

def upload_to_s3(data,s3_key):
    s3=boto3.client("s3")
    body=json.dumps(data,ensure_ascii=False).encode("utf-8")

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )
def run_bronze_ingest():
    for year in YEAR:
        try:
            log.info(f"Starting with {year}")
            search_items=search_videos(year,TARGET_RESULTS)
            video_ids=[item["id"]["videoId"]for item in search_items if "videoId" in item["id"]]
            stats_dict=get_statistics(video_ids)

            log.info(f"{year} | search ids: {len(video_ids)} | videos.list returned: {len(stats_dict)}")

            final_items=list(stats_dict.values())


            key=f"bronze/youtube/music/year={year}/data.json"
            
            upload_to_s3(final_items,key)

            log.info(f"for {year}  {len(final_items)} videos uploaded to s3")
            
        except Exception as e:
            log.exception(f"Something went wrong ,an error in {year}:{e}")

            
if __name__ == "__main__":
    run_bronze_ingest()
import pandas as pd
import re
import json
import boto3
from config import BUCKET
from logger import get_logger

log=get_logger("silver")

BRONZE_PREFIX=("bronze/youtube/music/")
SILVER_PREFIX=("silver/youtube/music/")

def list_json_keys(bucket:str ,prefix:str)->list[str]:
    s3=boto3.client("s3")
    keys=[]
    paginator=s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket,Prefix=prefix):
        for obj in page.get("Contents",[]):
            k=obj["Key"]
            if k.endswith(".json"):
                keys.append(k)
    return keys
def extract_year_from_key(key:str):
    m=re.search(r"year=(\d{4})",key)
    return int(m.group(1)) if m else None

def load_bronze_json(bucket:str,key:str)->list[str]:
 try:
    s3=boto3.client("s3")
    obj=s3.get_object(Bucket=bucket,Key=key)
    return json.load(obj["Body"])
 except json.JSONDecodeError:
    log.error(f"Error!,the file {key} is not in a valid json format")
    return[]
 except s3.exceptions.NoSuchKey:
    log.error(f"Error!,the file {key}is not found in s3 ")
    return[]
 except Exception as e:
    log.error(f"Unexpected error while reading {key}:{e}")
    return[]


def clean_and_flatten(items:list,year:int)->pd.DataFrame:
   if not items:
      return pd.DataFrame()
   
   df=pd.json_normalize(items)

   rename_map={
      "id":"video_id",
      "snippet.title":"title",
      "snippet.publishedAt":"published_at",
      "snippet.channelId":"channel_id",
      "snippet.channelTitle":"channel_name",
      "snippet.description":"description",
      "statistics.viewCount":"view_count",
      "statistics.likeCount":"like_count",
      "statistics.commentCount":"comment_count"

   }
   df=df.rename(columns=rename_map)

   df.columns=[c.strip().lower().replace(".","_").replace(" ","_")for c in df.columns]
   df['year']=year

   if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    
   numeric_cols=[c for c in df.columns if c.endswith("_count")]
   if numeric_cols:
    for col in numeric_cols:
      
      df[col]=pd.to_numeric(df[col],errors="coerce").fillna(0).astype("int64")

    if "video_id" in df.columns:
       df=df.drop_duplicates(subset=["video_id"])

    if "snippet_tags" in df.columns:
       df["snippet_tags"]=df["snippet_tags"].apply(lambda x:",".join(x)if isinstance(x,list)else str(x) )
    
    important_cols=["year","video_id","title","channel_name","view_count","like_count","comment_count","snippet_tags"]
    final_selection=[c for c in important_cols if c in df.columns]
    return df[final_selection]
   
def parquet_to_s3(df:pd.DataFrame,bucket:str,prefix:str,year:int):
    if df.empty:
      log.warning(f"Dataframe is empty, year{year}not possible to upload to s3")
      return
   
    full_path=f"s3://{bucket}/{prefix}"
    try:
         df.to_parquet(
            full_path,
            engine="pyarrow",
            index=False,
            compression="snappy",
            use_dictionary=False,
            partition_cols=["year"],
            storage_options={}
            
         )
         log.info(f"year:{year}--rows:{len(df)}silver layer updated succesfully in {full_path} ")
    except Exception as e:
         log.error(f"Error uploading to s3:{e}")

def run_silver_transform():
   print("running silver")
   keys=list_json_keys(BUCKET,BRONZE_PREFIX)
   log.info(f"{len(keys)} JSON files found in bronze s3")
   print("keys found",keys)

   if not keys:
      log.warning("there are not files to clean and transform in bronze s3bucket")
      return
   for key in keys :
      year=extract_year_from_key(key)
      if not year:
         log.warning(f"Not possible to extract year from:{key} ")
         continue
      log.info(f"Procesing {key} (year={year})")
      items=load_bronze_json(BUCKET,key)
      df=clean_and_flatten(items,year)
      parquet_to_s3(df,BUCKET,SILVER_PREFIX,year)

if __name__ == "__main__":
    run_silver_transform()

    
   







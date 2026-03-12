import os 
from dotenv import load_dotenv

load_dotenv()

BUCKET=os.getenv("S3_BUCKET","enana-datalakehouse26")
YT_API_KEY=os.getenv("YOUTUBE_API_KEY")
if not YT_API_KEY:
    raise ValueError("Missing auth key in env.")



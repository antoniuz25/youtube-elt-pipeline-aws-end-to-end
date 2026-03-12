import pandas as pd 
import re
import unicodedata
from config import BUCKET
from silver_transform import SILVER_PREFIX
from logger import get_logger

log=get_logger("gold")

GOLD_PREFIX="gold/youtube/music"

df = pd.read_parquet(
    f"s3://{BUCKET}/{SILVER_PREFIX}/",
    engine="pyarrow"
)
df['engagement_rate']=(
    (df['like_count']+df['comment_count'])
    /df['view_count']
    ).fillna(0)

top_videos=(
    df.sort_values(['year','view_count'],ascending=[True,False])
    .groupby('year')
    .head(5))
top_videos.to_parquet(f"s3://{BUCKET}/{GOLD_PREFIX}/top_videos/",engine='pyarrow',index=False)

kpis=(df.groupby('year').agg(
        total_videos=('video_id','count'),
        total_views=('view_count','sum'),
        avg_views=('view_count',"mean"),
        total_likes=('like_count','sum'),
        total_comments=('comment_count','sum'),
        avg_engagement=('engagement_rate','mean')
        )
        .reset_index()
    )
kpis.to_parquet(f"s3://{BUCKET}/{GOLD_PREFIX}/kpis/",engine="pyarrow",index=False)

def clean_text(text):
    if not text:return ""
    text=unicodedata.normalize('NFD',str(text).lower())
    text="".join(c for c in text if unicodedata.category(c)!='Mn')
    text=re.sub(r'[^a-z0-9\s-]',"",text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


GENRE_PATTERNS=[
    ("k_pop", re.compile(r"\b(k[\s-]?pop)")),
    ("reggaeton", re.compile(r"\b(reggaet[on]|perreo|dembow|urbano)\b",re.IGNORECASE)),
    ("hip_hop", re.compile(r"\b(hip[\s-]?hop|hip|rap|trap|r&b|street)\b",re.IGNORECASE)),
    ("salsa",re.compile(r"\b(salsa|timba|guaracha|mambo|guaguanco)\b",re.IGNORECASE)),
    ("pop",re.compile(r"\b(pop |indie[\s-]?pop|indie)\b",re.IGNORECASE)),
    ("rock",re.compile(r"\b(rock|metal|punk|grunge|heavy)\b",re.IGNORECASE)),
    ("electro",re.compile(r"\b(electronica|edm|house|dubstep|trance|dj)\b",re.IGNORECASE)),
    ("cumbia",re.compile(r"\b(cumbia|chicha|villera)\b",re.IGNORECASE)),
    ("brazilian_funk", re.compile(r"\b(kondzilla|funk|carioca)\b",re.IGNORECASE)),
    ("indian", re.compile(r"\b(assamese|hindi|bollywood|desi)\b",re.IGNORECASE)),
    ("romantic", re.compile(r"\b(love|romantic|sad|balada)\b",re.IGNORECASE)),
    ("instrumental", re.compile(r"\b(piano|guitar|instrumental)\b",re.IGNORECASE)),
    ("lofi", re.compile(r"\b(lofi|chill)\b",re.IGNORECASE)),
    ("anime", re.compile(r"\b(anime|amv|opening|ending)\b",re.IGNORECASE)),
    ("ambient", re.compile(r"\b(relax|sleep|study|ambient|meditation)\b",re.IGNORECASE)),
    ("meditation", re.compile(r"\b(meditation|sleep|relax|healing|369hz|432hz)\b",re.IGNORECASE)),
    ("ambient", re.compile(r"\b(ambient|study|piano|instrumental)\b",re.IGNORECASE)),
    ("latin", re.compile(r"\b(balada|latino|spanish|cumbia|salsa)\b",re.IGNORECASE)),
    ("religious", re.compile(r"\b(liturgia|worship|gospel|church)\b",re.IGNORECASE)),
    ("russian", re.compile(r"[а-яА-Я]",re.IGNORECASE)),

]
df['text_clean']=(df['title'].fillna("") +" "+ df['snippet_tags'].fillna("")).apply(clean_text)

df['genre']="other"


for genre,pattern in GENRE_PATTERNS:
    mask=(df['genre']=="other")& df['text_clean'].str.contains(pattern,na=False)
    df.loc[mask,'genre']=genre

top5_genre_by_year=(df.groupby(["year",'genre'])
                   .agg(total_views=('view_count','sum'))
                   .reset_index()
                   .sort_values(['year','total_views'],ascending=[True,False])
                   .groupby('year')
                   .head(5)
)
top5_genre_by_year.to_parquet(f"s3://{BUCKET}/{GOLD_PREFIX}/top_genre_by_year/",engine="pyarrow",index=False)
log.info(f"Saved top genre by year to s3 ,rows{len(top5_genre_by_year)}")

music_trends = (
    df.groupby(["year","genre"])
    .agg(
        videos=("video_id","count"),
        total_views=("view_count","sum"),
        avg_views=("view_count","mean"),
        total_likes=("like_count","sum"),
        total_comments=("comment_count","sum"),
        avg_engagement=("engagement_rate","mean")
    )
    .reset_index()
)
music_trends = music_trends.sort_values(["year","total_views"],ascending=[True,False]
)
music_trends.to_parquet(f"s3://{BUCKET}/{GOLD_PREFIX}/music_trends/",engine="pyarrow",index=False)

coverage = (
    df.groupby("year")
      .agg(
          total_views=("view_count", "sum"),
          other_views=("view_count", lambda x: x[df.loc[x.index, "genre"].eq("other")].sum()),
      )
      .reset_index()
)
coverage["other_views_share"] = coverage["other_views"] / coverage["total_views"]
coverage["classified_views_share"] = 1 - coverage["other_views_share"]

coverage.to_parquet(
    f"s3://{BUCKET}/{GOLD_PREFIX}/genre_coverage/",
    engine="pyarrow",
    index=False
)

log.info("the completly data for analytics was already updated successfully to s3 gold layer")


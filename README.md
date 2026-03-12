# youtube-elt-pipeline-aws-end-to-end
ELT pipeline project for analytics features using 2015-2019 youtube data to be stored in a DATA LAKE  on AWS


##ARCHITECTURE
<img width="1475" height="906" alt="ELT PIPELINE ARCHITECTURE" src="https://github.com/user-attachments/assets/fe7a4b17-619f-417e-a41c-a0cc5f78089a" />


##PROJECT OVERVIEW 
This project builds an end to end pipeline to analyze Youtube music trends between years 2015 -2019 using medallion architecture

This pipeline extracts data from Youtube V3 api,
stores raw  responses in an aws DATA LAKE then    
transforms data using pandas library 
and produces analytical dataset for visualization in Power bi


##MEDALLION ARCHITECTURE 
-BRONZE LAYER:Stores raw data responses from api without transformation ,all files stored as .json 
-SILVER LAYER:Take raw data from bronze to transform records into structured datasets ,normalizing numeric metrics and extract useful attributes
-GOLD LAYER :Creates analytics ready dataset for reporting and dashboard 


##TECH STACK
-Python 
-Youtube v3 api
-Amazon Web Services S3
-Pandas
-PyArrow


##DATA PIPELINE FLOW 
<img width="782" height="102" alt="flow chart pipeline yt" src="https://github.com/user-attachments/assets/ec8208b1-12df-42eb-84f1-5654af17cbf5" />


##GOLD DATASET

-kpis              --Yearly aggregated metrics including total views, total likes, total comments,
average engagement rate, and number of videos

-music_trends      --Yearly performance of music genres including average views,
average engagement, and total interactions

-top_videos        --Top 10 most popular music videos per year with channel name,
video title, view count, and publication year

-top_genre_by_year --Top performing genres by total views for each year in the dataset

-genre_coverage   --Distribution of classified vs unclassified views to evaluate genre classification coverage

##DASHBOARD
Overview Dashboard
<img width="1741" height="1051" alt="image" src="https://github.com/user-attachments/assets/b6e50931-9224-41b0-814d-7fd9ec321f6e" />

Top videos 2015-2019
<img width="1733" height="806" alt="image" src="https://github.com/user-attachments/assets/5fa3e5ae-31b4-4b59-a5c0-379854b11dd9" />

Genre Coverage
<img width="1688" height="983" alt="image" src="https://github.com/user-attachments/assets/5644662a-5b1f-4b4f-886e-04134c863af4" />


##KEY INSIGHTS

-2019 had the highest total views in the dataset

-Instrumental and electronic music showed strong growth in later years

-A small number of viral videos concentrated a large share of total views

-Genre classification coverage varied across years due to multilingual content

##HOW TO RUN 


Install required dependencies:

Clone the repository
Create a .env file with your AWS credentials and YouTube API key
Run the pipeline scripts:

python bronze_ingest.py
python silver_transform.py
python gold_analytics.py



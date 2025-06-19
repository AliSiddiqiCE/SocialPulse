import os
import pandas as pd
from sqlalchemy import create_engine

# Retrieve the database URL from environment variables
database_url = os.environ["DATABASE_URL"]

# Create a SQLAlchemy engine
engine = create_engine(database_url)

# Example: Read data from a CSV file
insta_hash = pd.read_csv("Insta_new_nexthashtags_cleaned.xlsx_csv.csv")

insta_hash.to_sql('nextretail_instagram_hashtag',
                  engine,
                  if_exists='append',
                  index=False)

insta_off = pd.read_csv("Insta_new_nextofficial_cleaned_2.xlsx_csv.csv")

insta_off.to_sql('nextretail_instagram_official',
                 engine,
                 if_exists='append',
                 index=False)

tik_hash = pd.read_csv(
    "dataset_tiktok-hashtag_NextRetail_cleaned.xlsx_csv.csv")

tik_hash.to_sql('nextretail_tiktok_hashtag',
                engine,
                if_exists='append',
                index=False)

tik_off = pd.read_csv("tiktok_NEXT_Official_cleaned.xlsx_csv.csv")

tik_off.to_sql('nextretail_tiktok_official',
               engine,
               if_exists='append',
               index=False)

yt_hash = pd.read_csv("dataset_youtube-Hashtag_Next 1_cleaned.xlsx_csv.csv")

yt_hash.to_sql('nextretail_youtube_hashtag',
               engine,
               if_exists='append',
               index=False)

yt_off = pd.read_csv(
    "dataset_youtube-channel-scraper_NextRetail-Official 1_cleaned.xlsx_csv.csv"
)

yt_off.to_sql('nextretail_youtube_official',
              engine,
              if_exists='append',
              index=False)

import os
import pandas as pd
from sqlalchemy import create_engine

# Retrieve the database URL from environment variables
database_url = os.environ["DATABASE_URL"]

# Create a SQLAlchemy engine
engine = create_engine(database_url)

# Example: Read data from a CSV file
df = pd.read_csv("dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv.csv")

# Optional: Convert date columns if necessary
# df['date_column'] = pd.to_datetime(df['date_column'], format='%d-%m-%Y')

# Insert data into the PostgreSQL table
df.to_sql('dataset_youtube-Hashtag_M&S 1_cleaned.xlsx_csv',
          engine,
          if_exists='append',
          index=False)

df2 = pd.read_csv("dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv.csv")

df2.to_sql('dataset_tiktok-hashtag_M&S_cleaned.xlsx_csv',
           engine,
           if_exists='append',
           index=False)

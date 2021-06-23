""" Find new movie reviews from the NYT """
import requests
import json
from datetime import date, timedelta
import pandas as pd
import numpy as np
from api_info import API_KEY

# NYT movie reviews base url
MOVIES_BASE_URL = "https://api.nytimes.com/svc/movies/v2/"
YESTERDAYS_DT = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

def connect_to_API(publication_dt: str) -> str:
    """
    NYT top movie reviews API connection to be re-used across functions.
    API call will return 20 most recent movies reviews from the NYT. 
    """
    payload = {"api-key": API_KEY, "publication-date": publication_dt}
    r = requests.get(f"{MOVIES_BASE_URL}reviews/all.json", params=payload)
    return r

def retrieve_movies_data(r) -> pd.DataFrame:
    """
    Use NYT API to find movie reviews including title, critic, link, and description.
    Change the recommend column to int type for testing purposes.
    Return pandas df.
    """
    data = r.json()
    movies = data['results']
    title, critic, description, recommend, opening_date, publication_date, mpaa_rating, nyt_review_url = ([] for i in range(8))
    for movie in movies:
        title.append(movie['display_title'])
        critic.append(movie['byline'])
        description.append(movie['summary_short'])
        recommend.append(movie['critics_pick'])
        opening_date.append(movie['opening_date'])
        publication_date.append(movie['publication_date'])
        mpaa_rating.append(movie['mpaa_rating'])
        nyt_review_url.append(movie['link']['url'])
    
    movies_df = pd.DataFrame(np.column_stack([title, critic, description, recommend, opening_date,\
                                          publication_date, mpaa_rating, nyt_review_url]), 
                                columns=["title", "critic", "description", "recommend", "opening_date",\
                                         "publication_date","mpaa_rating", "nyt_review_url"])

    movies_df['recommend'] = movies_df['recommend'].astype(int)

    return movies_df

def filter_movies_data(publication_dt: str) -> pd.DataFrame:
    """
    Filter movies data for specific date range. 
    Used to filter for last week only as the refresh frequency will be weekly.
    Returns pandas df.
    """
    pass

def check_if_valid_data(df: pd.DataFrame) -> bool:
    """
    Data validation used before proceeding to load stage.
    Check if there is data, if the primary key is unique, and if dates match. 
    """
    if df.empty:
        print("No movie reivews found. Finishing execution")
        return False 

    if pd.Series(df['title']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated, titles are not unqiue")
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # add a check for dates here 

    return True

def load_movies_data(publication_dt: str):
    """
    Load movies df into csv file into S3 bucket.
    Before loading, use check_if_valid_data function to validate results.
    """
    final_df = filter_movies_data(publication_dt)

    # Validate results
    if check_if_valid_data(final_df):
        print("Data valid, proceed to Load stage")

    # set S3 parameters and then load into bucket 
    bucket = "nyt-api-bucket"
    folder = "uploads"
    file_name = "NYT_movie_review_data.csv"

    try:
        path1 = f"s3://{bucket}/{folder}/{file_name}"
        final_df.to_csv(path1, index=False)
        print("Df exported successfully")
    except Exception as e:
        print(f"{e} \nData not exported, please check errors")

r = connect_to_API("2021-06-01:2021-06-22")
df = retrieve_movies_data(r)
print(df)
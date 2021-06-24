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

def filter_movies_data() -> pd.DataFrame:
    """
    Filter movies data for last week as the ETL frequency will be weekly.
    Returns desired pandas df.
    """
    yesterdays_dt = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    week_ago = (date.today() - timedelta(7)).strftime('%Y-%m-%d')
    # API date filter is start_dt:end_dt and uses the following format "YYYY-MM-DD:YYYY-MM-DD"
    date_filter = f'{week_ago}:{yesterdays_dt}'

    r = connect_to_API(date_filter)
    movies_df = retrieve_movies_data(r)

    return(movies_df)

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

    # Check if there are publication dates in the date range selected 
    date_list = []
    for day in range(1,8):
        date_list.append((date.today() - timedelta(day)).strftime('%Y-%m-%d'))
    last_week_set = set(date_list)
    timestamps = set(df["publication_date"].tolist())
    # check if there is intersection between dates in the last week and the set of publication dates
    if not (last_week_set & timestamps):
        raise Exception("None of the dates returned match the dates selected")

    return True

def load_movies_data():
    """
    Load movies df into csv file into S3 bucket.
    Before loading, use check_if_valid_data function to validate results.
    """
    final_df = filter_movies_data()

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

load_movies_data()
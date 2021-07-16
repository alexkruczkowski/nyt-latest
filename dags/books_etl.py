""" Find the bestselling novels from the NYT """
import requests
import json
from datetime import date, timedelta
import pandas as pd
import numpy as np
import airflow.hooks.S3_hook
from api_info import API_KEY

# NYT books base url, endpoints include book reviews and best seller lists
BOOKS_BASE_URL = "https://api.nytimes.com/svc/books/v3/"
YESTERDAYS_DT = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

def connect_to_API(api_url, dt = YESTERDAYS_DT) -> str:
    """
    NYT bestselling books API connection to be re-used across functions.
    """
    payload = {"api-key": API_KEY}
    r = requests.get(f"{BOOKS_BASE_URL}lists/{dt}/{api_url}.json", params=payload)
    return r

def retrieve_books_data(r) -> pd.DataFrame:
    """
    Use NYT API to find books data including title, author, ranking info, and description.
    Return pandas df.
    """
    data = r.json()
    books = data['results']['books']
    bestsellers_date = data['results']['bestsellers_date']
    #create lists for books results to then insert into df
    titles, authors, descriptions, ranks, last_week_ranks, weeks_on_list, bestseller_date, amazon_url = ([] for i in range(8))
    #insert all the book info into the relevant lists
    for book in books:
        titles.append(book['title'])
        authors.append(book['author'])
        descriptions.append(book['description'])
        ranks.append(book['rank'])
        last_week_ranks.append(book['rank_last_week'])
        weeks_on_list.append(book['weeks_on_list'])
        bestseller_date.append(bestsellers_date)
        amazon_url.append(book['amazon_product_url'])

    #convert lists into pandas df
    books_df = pd.DataFrame(np.column_stack([titles, authors, descriptions, ranks, last_week_ranks,\
                                             weeks_on_list, bestseller_date, amazon_url]), 
                                columns=["titles", "authors", "descriptions", "ranks", "last_week_ranks",\
                                         "weeks_on_list","bestseller_date", "amazon_url"])

    return books_df

def combine_books_data(dt = YESTERDAYS_DT) -> pd.DataFrame:
    """
    Combine fiction and non fictions bestselling lists (updated weekly).
    Output one pandas df with complete the top 15 books from each category.
    """
    fiction_url = "combined-print-and-e-book-fiction"
    non_fiction_url = "combined-print-and-e-book-nonfiction"

    # dt variable is included to add the ability to pull results for diff weeks
    r_fiction = connect_to_API(fiction_url, dt)
    r_non_fiction = connect_to_API(non_fiction_url, dt)

    df_fiction = retrieve_books_data(r_fiction)
    df_non_fiction = retrieve_books_data(r_non_fiction)
    
    combined_df = pd.concat([df_fiction, df_non_fiction], ignore_index=True)

    # Convert date column to datetime for use in validation
    combined_df['bestseller_date'] = pd.to_datetime(combined_df['bestseller_date'])
    
    return(combined_df)

def check_if_valid_data(df: pd.DataFrame) -> bool:
    """
    Data validation used before proceeding to load stage.
    Check if there is data and if the primary key is unique. 
    """
    if df.empty:
        print("No books found. Finishing execution")
        return False 

    if pd.Series(df['titles']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated, titles are not unqiue")
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True
    
def load_books_data():
    """
    Load books df into csv file into S3 bucket in airflow using S3 hook.
    Before loading, use check_if_valid_data function to validate results.
    """
    final_df = combine_books_data()

    # Validate results
    if check_if_valid_data(final_df):
        print("Data valid, proceed to Load stage")

    # Set S3 parameters and then load into bucket 
    bucket = "nyt-api-bucket"
    folder = "uploads"
    file_name = "NYT_bestseller_data.csv"
    key = f"{folder}/{file_name}"

    try:
        # Convert df to csv, upload to S3 in airflow
        final_df.to_csv(file_name, index=False, encoding="utf-8")
        hook = airflow.hooks.S3_hook.S3Hook('my_S3_conn')
        hook.load_file(bucket_name=bucket, filename=file_name, key=key, replace=True)
        print("Df exported successfully")
    except Exception as e:
        print(f"{e} \nData not exported, please check errors")


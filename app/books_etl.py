""" Find the latest top stories, bestselling novels, and new movie reviews from the NYT """
import requests
from api_info import API_KEY

# NYT books base url, endpoints include book reviews and best seller lists
BOOKS_BASE_URL = "https://api.nytimes.com/svc/books/v3/"

def check_if_valid_data(books_dataframe):
    """
    Data validation used before proceeding to load stage.
    Check if there is data, if the primary key is unique, and if dates match. 
    """
    pass

def retrieve_books_data():
    """
    Use NYT API to find desired books data. Return JSON data.
    """
    payload = {"api-key": API_KEY}
    r = requests.get(f"{BOOKS_BASE_URL}lists/overview.json", params=payload)
    data = r.json()
    return(data)

def clean_books_data(json_input):
    """
    Manipulate data into desired format, return pandas DF. 
    """
    pass

def load_books_data(books_dataframe):
    """
    Temporarily load into a local CSV file, later load into S3 bucket.
    Before loading, use check_if_valid_data function to validate results.
    """
    pass

# testing return functionality, add to tests
data = retrieve_books_data()
print(data)
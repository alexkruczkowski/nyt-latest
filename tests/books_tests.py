""" 
Tests for NYT books API 
run using python -m pytest file_name.py
"""
import pytest 
import requests
import pandas as pd
from datetime import date, timedelta
from app.books_etl import connect_to_API, retrieve_books_data, combine_books_data, check_if_valid_data
from api_info import API_KEY

# NYT books base url, endpoints include book reviews and best seller lists
BOOKS_BASE_URL = "https://api.nytimes.com/svc/books/v3/"

# Global variables used for testing
TEST_DT = '2021-06-20'
data = {
    'titles':'FREED',
    'authors':'E.L. James',
    'descriptions':'The final chapter of the Fifty Shades as Told by Christian trilogy delves into the wedding between Christian Grey and Anastasia Steele.',
    'ranks':'1',
    'last_week_ranks':'0',
    'weeks_on_list':'1',
    'bestseller_date':'2021-06-05',
    'amazon_url':'https://www.amazon.com/dp/1728251036?tag=NYTBSREV-20'
}
TEST_DF = pd.DataFrame(data, index=[0])

def test_get_fiction_bestsellers_check_status_code_equals_200():
    r_fiction = connect_to_API("combined-print-and-e-book-fiction")
    assert r_fiction.status_code == 200

def test_get_nonfiction_bestsellers_check_status_code_equals_200():
    r_non_fiction = connect_to_API("combined-print-and-e-book-nonfiction")
    assert r_non_fiction.status_code == 200

def test_retrieve_book_data_check_content_equals_expected():
    r_test_fiction = connect_to_API("combined-print-and-e-book-fiction", TEST_DT)
    fiction_df = retrieve_books_data(r_test_fiction)
    fiction_df = fiction_df.iloc[0:1, :] 
    pd.testing.assert_frame_equal(fiction_df, TEST_DF)

def test_combine_books_data_check_content_equals_expected():
    combined_df = combine_books_data(TEST_DT)
    combined_df = combined_df.iloc[0:1, :] 
    test_df = TEST_DF['bestseller_date'] = pd.to_datetime(TEST_DF['bestseller_date'])
    pd.testing.assert_frame_equal(combined_df, TEST_DF)
    
def test_check_data_returns_false_for_empty_df():
    empty_df = pd.DataFrame()
    assert check_if_valid_data(empty_df) == False

def test_check_data_raises_exception_for_null_values():
    df_with_null_values = pd.DataFrame(columns = ["titles", "authors", "descriptions", "ranks", "last_week_ranks",\
                                         "weeks_on_list","bestseller_date", "amazon_url"],index=[0])
    with pytest.raises(Exception) as execinfo:   
        check_if_valid_data(df_with_null_values)
    assert str(execinfo.value) == 'Null values found'

def test_check_data_on_valid_df_return_true():
    assert check_if_valid_data(TEST_DF) == True
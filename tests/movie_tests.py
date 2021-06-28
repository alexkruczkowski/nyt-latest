""" 
Tests for NYT movies API 
run using python -m pytest file_name.py
"""
import pytest
import requests
import pandas as pd
from datetime import date, timedelta
from dags.movies_etl import connect_to_API, retrieve_movies_data, check_if_valid_data
from dags.api_info import API_KEY

# NYT movie reviews base url
MOVIES_BASE_URL = "https://api.nytimes.com/svc/movies/v2/"

# Used for comparing dataframe output with expected
data = {
    'title':'The Birthday Cake',
    'critic':'Jeannette Catsoulis',
    'description':'This mob drama folds family secrets and fading power into a story of operatic vengeance.',
    'recommend':1,
    'opening_date':'2021-06-18',
    'publication_date':'2021-06-17',
    'mpaa_rating':'R',
    'nyt_review_url':'https://www.nytimes.com/2021/06/17/movies/the-birthday-cake-review.html'
}
TEST_DF = pd.DataFrame(data, index=[0])

def test_get_movie_reviews_check_status_code_equals_200():
    r_movies = connect_to_API("2021-06-01:2021-06-17")
    assert r_movies.status_code == 200

def test_retrieve_movie_data_check_content_equals_expected():
    r_test_movies = connect_to_API("2021-06-01:2021-06-17")
    movies_df = retrieve_movies_data(r_test_movies)
    movies_df = movies_df.iloc[0:1, :] 
    pd.testing.assert_frame_equal(movies_df, TEST_DF)

def test_check_data_returns_false_for_empty_df():
    empty_df = pd.DataFrame()
    assert check_if_valid_data(empty_df) == False

# Return to this test to re-factor

# def test_check_data_raises_exception_for_null_values():
#     df_with_null_values = pd.DataFrame(columns = ["title", "critic", "description", "recommend", "opening_date",\
#                                          "publication_date","mpaa_rating", "nyt_review_url"],index=[0])
#     with pytest.raises(Exception) as execinfo:   
#         check_if_valid_data(df_with_null_values)
#     assert str(execinfo.value) == 'Null values found'

def test_check_data_on_valid_df_return_true():
    assert check_if_valid_data(TEST_DF) == True
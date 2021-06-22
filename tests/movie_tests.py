""" 
Tests for NYT movies API 
run using python -m pytest file_name.py
"""
import pytest
import requests
import pandas as pd
from datetime import date, timedelta
from app.movies_etl import connect_to_API
from api_info import API_KEY

# NYT movie reviews base url
MOVIES_BASE_URL = "https://api.nytimes.com/svc/movies/v2/"

def test_get_movie_reviews_check_status_code_equals_200():
    r_movies = connect_to_API()
    assert r_movies.status_code == 200
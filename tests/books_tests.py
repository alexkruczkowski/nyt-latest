""" 
Tests for NYT books API 
run using python -m pytest file_name.py
"""
import pytest 
import requests
from datetime import date, timedelta
from app.books_etl import connect_to_API, retrieve_books_data
from api_info import API_KEY

# NYT books base url, endpoints include book reviews and best seller lists
BOOKS_BASE_URL = "https://api.nytimes.com/svc/books/v3/"
YESTERDAYS_DT = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

def test_get_fiction_bestsellers_check_status_code_equals_200():
    r_fiction = connect_to_API("combined-print-and-e-book-fiction")
    assert r_fiction.status_code == 200

def test_get_nonfiction_bestsellers_check_status_code_equals_200():
    r_non_fiction = connect_to_API("combined-print-and-e-book-nonfiction")
    assert r_non_fiction.status_code == 200

def test_retrieve_book_data_check_content_equals_expected():
    pass


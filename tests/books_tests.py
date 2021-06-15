""" Tests for NYT books API """
import pytest 
import requests
from api_info import API_KEY

# NYT books base url, endpoints include book reviews and best seller lists
BOOKS_BASE_URL = "https://api.nytimes.com/svc/books/v3/"

def get_bestsellers_overview_check_status_code_equals_200():
    payload = {"api-key": API_KEY}
    r = requests.get(f"{BOOKS_BASE_URL}lists/overview.json", params=payload)
    assert r.status_code == 200

def get_bestsellers_overview_check_content_type_equals_json():
    pass


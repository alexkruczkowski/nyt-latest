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

def connect_to_API() -> str:
    """
    NYT top movie reviews API connection to be re-used across functions.
    """
    payload = {"api-key": API_KEY}
    r = requests.get(f"{MOVIES_BASE_URL}reviews/picks.json", params=payload)
    return r



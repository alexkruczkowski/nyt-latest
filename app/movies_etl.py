""" Find the latest top stories, bestselling novels, and new movie reviews from the NYT """
import requests
from api_info import API_KEY

# NYT movies base URL
MOVIES_BASE_URL = "https://api.nytimes.com/svc/movies/v2/"
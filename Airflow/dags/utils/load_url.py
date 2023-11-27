import requests

def load_url(url,**kwargs):
    response = requests.get(url)
    # Process the response as needed
    print(response.text)
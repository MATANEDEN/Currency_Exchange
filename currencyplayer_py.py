import  requests
from dotenv import load_dotenv
import  os

class MakeRequest(object):
    load_dotenv()
    def __init__(self, params, api_url=os.getenv("API_URL")):
        self.api_url = api_url
        self.params = params

    def request(self):
        self.request = requests.get(self.api_url, params = self.params)
        self.history_currencies = self.request.json()
        return self.history_currencies['quotes']



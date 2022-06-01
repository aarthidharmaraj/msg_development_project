"""This module authenticate api with api key and get 
historic datas and product lists from eBird Api based on given date"""
from datetime import date,datetime
import requests

class PullDataFromEBirdApi:
    """This class has methods to pull historic datas and product lists from 
    api with api key authentication based on the given date"""
    
    def __init__(self,logger,config):
        """This is the init method for the class PullDataFromEBirdApi"""
        self.logger=logger
        self.base_url=config["eBird_api_datas"]['basic_url']
        self.min_year = 1800
        self.year_limit = int(date.today().year) - 1
    
    def fetch_historical_data_from_api(self,date,region_code):
        """This method fetches historical data from api for the given date"""
        try:
            date=datetime.datetime.strptime(date, "%Y-%m-%d")
            if date.year in range(self.min_year, self.year_limit + 1):
                endpoint = f"data/obs/{region_code}/historic/{date.year}/{date.month}/{date.day}"
                request_url = self.base_url + endpoint
                response = requests.get(request_url)
                if response.status_code == 200:
                    responses = response.json()
                    self.logger.info(
                        f"Got the response from api for given date with status{response.status_code}"
                    )
                else:
                    print("Cannot get the response from api", response.status_code)
                    self.logger.error(
                        "It gives a failure response with code %s", response.status_code
                    )
                    responses = None
            else:
                self.logger.error(f"No responses are available for given year%s",date.year)
                print("No responses are available for given year",date.year)
                responses = None

        except Exception as err:
            self.logger.error(f"Cannot get the response from api due to problem in api{err}")
            print("No response from api", err)
            responses = None
        return responses

        
        
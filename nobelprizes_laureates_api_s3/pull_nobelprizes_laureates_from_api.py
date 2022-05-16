"""This module fetches the nobel prize and laureates data from api 
"https://api.nobelprize.org/2.0/" and return the responses """

import os
import configparser
import requests
# import pandas as pd

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class NobelprizeLaureatesFromApi:
    """This class has methods to get response for Nobel prizes and laureates
    from the api for the given year"""

    def __init__(self, logger):
        """This is the init method for the class NobelprizeLaureatesFromApi"""
        self.logger = logger
        self.base_url = config["nobelprize_laureates_api"]["basic_url"]

    def pull_nobelprizes_laureates_from_api(self, pull_for, startyear, endyear):
        """This method pulls the response for Nobel prizes from the api"""
        try:

            endpoint = f"{pull_for}?nobelPrizeYear={startyear}&yearTo={endyear}"
            request_url = self.base_url + endpoint
            # print(request_url)
            response = requests.get(request_url).json()
            # print(response)
            # df_data=pd.DataFrame(response['laureates'])
            # print(df_data)
        except Exception as err:
            self.logger.info("Cannot get the response from api due to problem in api")
            print("No response from api",err)
            response = None
        return response

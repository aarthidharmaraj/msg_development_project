""" This module is to pull data from api for each cities in USA"""
import sys
import requests


class PullDataFromMetaweatherApi:
    """This class pulls data from an api as per their woeid-where on earth id"""

    def __init__(self, logger):
        """This is the init method of the PullData class"""
        self.logger = logger

    def get_weather_data_cities_using_woeid_from_api(self, woeid, search_date):
        """This method used to get the details using the woeid of city from
        api "/api/location/(woeid)/(date)/" for the provided date.
        If any problem in api terminate the execution"""

        try:
            response = requests.get(
                "https://www.metaweather.com/api/location/{woeid}/{date}/".format(
                    woeid=woeid, date=search_date
                )
            ).json()
            self.logger.info("The Metaweather information of each city are got from the api")
            # print(response)
            return response

        except Exception as err:
            print("Cannot able to fetch the MetaWeather data of woeid due to Problems in api", err)
            self.logger.info(
                "Cannot able to fetch the MetaWeather data of woeid due to Problems in api"
            )
            sys.exit()

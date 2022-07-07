"""This method get the record of an object from metamuseum api for the given object_id and 
endpoints and region based on their status codes"""
import requests


class MetaMuseumApi:
    """This class has methods to get details from metamuseum Api for the given endpoint,
    object_id"""

    def __init__(self, section, logger):
        """This is the init method for the class MetaMseumApi"""
        self.logger = logger
        self.section = section

    def get_endpoint_for_object_record(self, object_id):
        """This method gets the endpoint for fetching the public holidays and long_weekend for
        given object_id and country
        parameters: object_id - object_id on which public metamuseum details are needed"""
        try:
            endpoint = f"objects/{object_id}"
            self.logger.info("Got %s endpoint for given %s object id", endpoint, object_id)
            response = self.get_response_from_api(endpoint)
        except Exception as err:
            self.logger.error("Cannot get the endpoint for %s and %s", object_id, err)
            response = None
        return response

    def get_response_from_api(self, endpoint):
        """This method gets the response from api for the given endpoint
        parameters: endpoint - endpoint for requests from api"""
        try:
            base_url = self.section["basic_url"]
            request_url = base_url + endpoint
            print(request_url)
            response = requests.get(request_url)
            if response.status_code != 200:
                self.logger.error("Cannot get the response from api with %s", response.status_code)
                response_json = None
            else:
                response_json = response.json()
                self.logger.info(
                    "Got the response from api for given object_id with status %s",
                    response.status_code,
                )
        except Exception as err:
            self.logger.error("Cannot get the response from api due to problem in api %s", err)
            print("No response from api", err)
            response_json = None
        return response_json

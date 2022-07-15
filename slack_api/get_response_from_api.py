"""This module has methods to get response from slack api for admin analytics data with the token authorization"""
import requests
from cryptography.fernet import Fernet


class SlackApi:
    """This class has methods to get response from Api with authentication"""
    
    def __init__(self,section,logger):
        """This is the init method for the class SlackApi"""
        self.logger=logger
        self.section=section
    
    def decrypt_api_key_from_config(self,encrypt_key,fernet_key):
        """This method gets the encrypted acess token from config, decrypt it and return the data"""
        try:
            fernet = Fernet(fernet_key)
            self.logger.info("Got the encrypted key from config %s", encrypt_key)
            decrypt_token = fernet.decrypt(encrypt_key.encode()).decode()
            self.logger.info("Successfully decrypted the key %s", decrypt_token)
        except Exception as err:
            decrypt_token = None
            self.logger.error("Cannot decrypt the api key from config %s", err)
        return decrypt_token

    def authenticate_api_with_token(self):
        """This method decrypt the access token from config file and create
        the header and authenticate the api with access token"""
        try:
            encrypt_key = self.section["access_token"]
            fernet_key = self.section["fernet_key"]
            user_access_token = self.decrypt_api_key_from_config(encrypt_key,fernet_key)
            header = {"token": user_access_token,'type':"member"} #currently supports member type
            self.logger.info("Authentication the api with api_key and passed it in header")
        except Exception as err:
            self.logger.error("Cannot generate the header with the given api_key %s", err)
            header = None
        return header
    
    def get_endpoint_for_admin_analytics_data(self,date):
        """This method is to get endpoint for fetching admin analytics data from api for given date
        parameters: date - the date for which the datas are needed"""
        try:
            endpoint = f"?date={date}"
            self.logger.info("Got %s endpoint for given date %s", endpoint, date)
            response = self.get_response_from_api(endpoint)
        except Exception as err:
            self.logger.error("Cannot get the endpoint for date %s and %s", date, err)
            response = None
        return response

    def get_response_from_api(self,endpoint):
        """This method gets the response from api for the given endpoint
        parameters: endpoint - endpoint for requests from api"""
        try:
            base_url = self.section["basic_url"]
            request_url = base_url + endpoint
            header=self.authenticate_api_with_token()
            response_json = requests.get(request_url,header=header)
            if response_json.status_code != 200:
                self.logger.error("Cannot get the response from api with %s", response_json.status_code)
                response_json = None
            else:
                self.logger.info(
                    "Got the response from api for given object_id with status %s",
                    response_json.status_code,
                )
        except Exception as err:
            self.logger.error("Cannot get the response from api due to problem in api %s", err)
            print("No response from api", err)
            response_json = None
        return response_json
         
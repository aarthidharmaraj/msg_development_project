"""This module gets the response from api with authentication of api key by decrypting it from cinfig for the given endpoints with pagination"""
import requests
import pandas as pd
from cryptography.fernet import Fernet
class PullDataFromFreeSoundApi:
    """This class has methods to pull similar sounds for soundid, user packs and user sounds for given username from api with authentication by decrypting api key from config"""
    
    def __init__(self,section,logger):
        """This is the init method for the class PullDataFromFreeSoundApi"""
        self.logger=logger
        self.section=section
    
    def decrypt_api_key_from_config(self,encrypt_key,fernet_key):
        """This method gets the encrypted api key from config, decrypt it and return the data"""
        try:
            fernet = Fernet(fernet_key)
            self.logger.info("Got the encrypted key from config %s", encrypt_key)
            decrypt_token = fernet.decrypt(encrypt_key.encode()).decode()
            self.logger.info("Successfully decrypted the key %s", decrypt_token)
        except Exception as err:
            decrypt_token = None
            self.logger.error("Cannot decrypt the api key from config %s", err)
        return decrypt_token
    
    def authenticate_api_with_key(self):
        """This method decrypt the api key from config file and create
        the query params and authenticate the api with api key"""
        try:
            encrypt_key = self.section["api_key"]
            fernet_key = self.section["fernet_key"]
            api_key_token = self.decrypt_api_key_from_config(encrypt_key,fernet_key)
            params= {"token": api_key_token}
            self.logger.info("Authenticated the api with api_key and passed it in params")
        except Exception as err:
            self.logger.error("Cannot generate the params with the given api_key %s", err)
            params = None
        return params

    
    def fetch_similar_sounds_from_api(self,sound_id):
        """This method fetches similar sounds for the given sound id from api by ceating an endpoint
        parameters:  sound_id - the id of the song for which similar sounds are needed"""
        try:
            endpoint=f"sounds/{sound_id}/similar/"
            self.logger.info("Getting the similar sound response from api with endpoint %s",endpoint)
            response=self.get_response_from_api_pagination(endpoint)
        except Exception as err:
            self.logger.error("Cannot get the endpoint for the given sound id %s",err)
            response=None
        return response
        
    def fetch_user_sounds_packs_from_api(self,username):
        """This method fetches the user sounds or user packs based on the endpoint value by creating an endpoint"""
        try:
            endpoints=f"users/{username}/packs/"
            self.logger.info("Getting the similar sound response from api with endpoint %s",endpoints)
            response=self.get_response_from_api_pagination(endpoints)
        except Exception as err:
            self.logger.error("Cannot get the endpoint for the given sound id %s",err)
            response=None
        return response
    
    def get_response_from_api_pagination(self,endpoint):
        """This method gets the response from api for the given endpoints with pagination using the next link"""
        try:
            chunk_data=[]
            base_url=self.section["basic_url"]
            params=self.authenticate_api_with_key() 
            request_url = base_url + endpoint
            while True:
                    print(request_url)
                    response = requests.get(request_url,params=params)
                    if response.status_code == 200:
                        response_json = response.json()
                        result_data=[filter(None,response_json["results"])]
                        self.logger.info(
                            f"Got the response from api for given year with status{response.status_code}"
                        )
                        df_data = pd.DataFrame(result_data,index=[0])
                        chunk_data.append(df_data)
                    #    and response_json['next']<=f"https://freesound.org/apiv2/sounds/636917/similar/?&weights=&target=636917&page=10":
                        if response_json['next'] :
                            request_url= response_json['next']
                            self.logger.info("Got the next url of similar sound endpoint")
                        else:
                            self.logger.info("The function has been breaked since it has no next link")
                            break
                    else:
                        print("It gives a failure response with code", response.status_code)
                        self.logger.error(
                            f"It gives a failure response with code {response.status_code}"
                        )
                        df_data = None
            df_data=pd.concat(chunk_data)
            # print(df_data)
        except Exception as err:
            self.logger.error(f"Cannot get the response from api due to problem in api{err}")
            print("No response from api", err)
            df_data = None
        return df_data
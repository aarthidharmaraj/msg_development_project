"""This module fetches the nobel prize and laureates data from api 
"https://api.nobelprize.org/2.0/" and return the response """

from datetime import date
import requests

class NobelprizeLaureatesFromApi:
    """This class has methods to get response for Nobel prizes and laureates
    from the api for the given year"""

    def __init__(self, logger,config):
        """This is the init method for the class NobelprizeLaureatesFromApi"""
        self.logger = logger
        self.base_url = config["nobelprize_laureates_api"]["basic_url"]
        self.year_limit = int(date.today().year) - 1
        self.min_year = 1901

    def pull_nobelprizes_laureates_from_api(self, pull_for,startyear,endyear):
        """This method pulls the response for Nobel prizes from the api"""
        try:
            NextToken = True
            responses_data=[]
            # year_range=self.min_year, self.year_limit + 1)
            if startyear in range(self.min_year, self.year_limit + 1) and endyear in range(self.min_year, self.year_limit + 1):
                endpoint = f"{pull_for}?nobelPrizeYear={startyear}&yearTo={endyear}"
                request_url = self.base_url + endpoint
                while NextToken is not None:
                    print(request_url)
                    response = requests.get(request_url)
                    if response.status_code == 200:
                        responses = response.json()
                        responses_data.append(responses)
                        self.logger.info(
                            f"Got the response from api for given year with status{response.status_code}"
                        )
                        if 'next' in responses['links']:
                            next_link = responses['links']['next']
                            request_url= next_link
                        else:
                            break
                    else:
                        print("It gives a failure response with code", response.status_code)
                        self.logger.error(
                            f"It gives a failure response with code {response.status_code}"
                        )
                        responses_data = None
            else:
                self.logger.error("Cannot fetch for the given years")
                print("No responses are available for given years")
                responses_data= None

        except Exception as err:
            self.logger.error(f"Cannot get the response from api due to problem in api{err}")
            print("No response from api", err)
            responses_data = None
        return responses_data
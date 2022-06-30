"""This module gets the sumrise and sunset data from api for the given date based on the given latitude and
longitude partition them based on year, month and date and upload to s3"""
from datetime import datetime, timedelta
import os
import argparse
import sys
import ast
from geopy.geocoders import Nominatim
import pandas as pd
from logger_path.logger_object_path import LoggerPath
from aws_s3.s3_details import S3Details
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api import SunriseSunsetApi

datas = LoggerPath.logger_object("sunrise_sunset_api")
current_date = datetime.now().date()


class SunriseSunsetDataUploadS3:
    """This class has methods to get sunrise and sunset data from api for the given date
    and location,partition them and upload to s3"""

    def __init__(self, startdate, enddate, latitude, longitude, city):
        """This is the init method for the class SunriseSunsetDataUpload3"""
        self.section = datas["config"]["sunrise_sunset_api"]
        self.logger = datas["logger"]
        self.startdate = startdate
        self.enddate = enddate
        self.latitude = latitude
        self.longitude = longitude
        self.city = city
        self.logger.info("Successfully created the instance of the class")

    def get_details_for_givendates(self):
        """This method is to get the datas for sunrise and sunset for the given dates"""
        try:
            # check the given dates ranges
            if self.enddate:
                if self.startdate < self.enddate:
                    dates = {"date1": self.enddate, "date2": self.startdate}
                    self.logger.info(
                        "Getting details from api for given startdate %s to enddate %s",
                        self.startdate,
                        self.enddate,
                    )
                else:
                    self.logger.info("Cannot fetch details as startdate is greater than enddate")
                    # sys.exit("script was terminated since startdate is greater than enddate")
            else:
                dates = {"date1": self.startdate, "date2": self.startdate}
                self.logger.info(
                    "Fetching datas from api between %s and current date %s",
                    self.startdate,
                    self.startdate,
                )
        except Exception as err:
            dates = None
            self.logger.info("Cannot get the details for the given dates %s", err)
            print("script was terminated when fetching for given dates")
        return dates

    def get_lat_long_city_from_config(self, date1, date2):
        """This method is to get the latitude, longitude and city name from config if it is not given by the user"""
        try:
            cities = self.section["cities"]
            available_cities = ast.literal_eval(cities)
            if self.city and self.latitude and self.longitude:
                if self.city in available_cities.keys():
                    self.logger.info("The given city %s is alrady available in config", self.city)
                    response=None
                    # sys.exit("System terminated as the given city details are available in config")
                else:
                    new_dict = {}
                    new_dict[self.city] = {}
                    new_dict[self.city]["lat"] = self.latitude
                    new_dict[self.city]["long"] = self.longitude
                    self.logger.info("Fetching details from api for user given city %s", self.city)
                    response = self.get_response_from_api(
                        date1, date2, self.city, self.latitude, self.longitude
                    )
                    self.update_config_if_not_exists(new_dict, available_cities)
            else:
                for city, data in available_cities.items():
                    print(city)
                    self.logger.info("Fetching details from api for cities in config %s", city)
                    response = self.get_response_from_api(
                        date1, date2, city, data.get("lat"), data.get("long")
                    )
        except Exception as err:
            self.logger.error("Cannot get the details from config %s", err)
            print("Cannot get lat, longitude and city details from config", err)
            response = None
        return response

    def get_response_from_api(self, date1, date2, city, latitude, longitude):
        """This method fetches the sunrise and sunset datas from api based on the date and locations
        parameters : date1 ,date 2 - the dates on which datas are needed
                city - the city for which datas are fetched based on their latitude and longitude"""
        try:
            api = SunriseSunsetApi(self.section, self.logger)
            day_count = (date1 - date2).days + 1
            print(day_count)
            for day in range(day_count):  # Fetch for the given dates one by one
                single_date = date2 + timedelta(day)
                print(single_date)
                self.logger.info("Getting response from api for %s id", single_date)
                response = api.get_endpoint_for_sunrise_sunset(single_date, latitude, longitude)
                self.get_dataframe_for_response(response, single_date, city)
        except Exception as err:
            self.logger.error("Cannot able to get response from api for the date- %s", err)
            response = None
            # sys.exit("System has terminated for fail in getting response from api")
        return response

    def get_dataframe_for_response(self, response, date, city):
        """This method gets response from api,convert to dataframe and create json file for that
        parameters : response - response from api
                     date - the date for which datas are fetched
                     city - the city for which datas are fetched"""
        try:
            if response is not None:
                self.logger.info("Got the response from api for %s", date)
                df_data = pd.DataFrame.from_dict(response["results"], orient="index")
                df_data = df_data.transpose()
                if not df_data.empty:
                    partition_path = self.put_partition_path(date, city)
                    file_name = f"sunrise_sunset_on_{date}_for_{city}.json"
                    self.create_json_file_partition(df_data, file_name, partition_path, date)
            else:
                self.logger.info("No responses from api for the %s", date)
                df_data = None
        except Exception as err:
            self.logger.error("Cannot create the dataframe for the given response %s", err)
            df_data = None
        return df_data

    def create_json_file_partition(self, df_data, file_name, partition_path, date):
        """This method creates a temporary json file,create partition path and upload to s3
        parameters : df_data - dataframe created from the response
                    file_name - file_name to be uploaded in s3
                    partition_path - partition based on obj_id
                    city - the area for which datas are fetched"""
        try:
            self.logger.info("Got the dataframe for the object id %s", date)
            s3_path = self.section["s3_path"]
            temp_s3 = ApiDataPartitionUploadLocal(self.logger)
            local_path = LoggerPath.local_download_path("sunrise_sunset_api_datas")
            df_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info("Created the json file for %s", date)
            copy_source = local_path + "/" + file_name
            file_name = temp_s3.upload_partition_s3_local(
                local_path,
                copy_source,
                file_name,
                s3_path + "/" + partition_path,
            )
            # file=self.upload_to_s3(s3_path,copy_source,s3_path  + "/" + partition_path, file_name)
        except Exception as err:
            self.logger.error("Cannot create a json file for file_name %s", file_name)
            print("Canot create a json file", err)
            file_name = None
        return file_name

    def put_partition_path(self, date, city):
        """This method will make partion path based on year,month and date
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = date.strftime(f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d")
            self.logger.info("Created the partition path for the given %s", date)
        except Exception as err:
            self.logger.error("Cannot made a partition for %s because of %s", date, err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on endpoint and date"""
        try:
            s3_client = S3Details(self.logger, datas["config"])
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            file = s3_client.upload_file(source, self.section["bucket"], bucket_path, key)
            print("the file has been uploaded to s3 in the given path")
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file

    def update_config_if_not_exists(self, new_dict, available_cities):
        """This method updates the cities in config if a new city details are given"""
        try:
            available_cities.update(new_dict)
            datas["config"].set("sunrise_sunset_api", "cities", str(available_cities))
            with open(datas["parent_dir"] + "/details.ini", "w", encoding="utf-8") as file:
                datas["config"].write(file)
            self.logger.info(
                "Successfully updated the given city details %s and its details in config", new_dict
            )
            update = "success"
        except Exception as err:
            self.logger.error("Cannot update the city details in config %s", err)
            print("Cannot update the city,latitude and longitude values in config", err)
            update = "failed"
        return update


def checkvalid_lat(lat):
    """This method checks the given latitide is a valid longitude value"""
    try:
        lat = float(lat)
        if -90 <= lat <= 90:
            valid_lat = lat
            datas["logger"].info("The given latitude %s is valid", lat)
        else:
            raise Exception
    except Exception as err:
        datas["logger"].error("The given latitude %s is not an valid one %s", lat, err)
        print(err)
        msg = f" {lat} is not valid latitude."
        valid_lat = None
        # raise argparse.ArgumentTypeError(msg)
    return valid_lat


def checkvalid_long(long):
    """This method checks the given longitude is a valid longitude value"""
    try:
        long = float(long)
        if -180 <= long <= 180:
            valid_long = long
            datas["logger"].info("The given longitude %s is valid", long)
        else:
            raise Exception
    except Exception as err:
        datas["logger"].error("The given longitude %s is not an valid one %s", long, err)
        msg = f" {long} is not valid longitude."
        valid_long = None
        # raise argparse.ArgumentTypeError(msg)
    return valid_long


def check_valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        date = datetime.strptime(date, "%Y-%m-%d").date()
        if date < datetime.now().date():  # checks with current date
            valid_date = date
            datas["logger"].info("The given date is a valid date %s", valid_date)
        else:
            datas["logger"].info("Cannot get datas from api for current and  future dates")
            raise Exception
    except (Exception, ValueError):
        datas["logger"].info(
            "%s not valid date.It must be in format YYYY-MM-DD and must be ended dates",
            date,
        )
        valid_date = None
        msg = f"{date} not valid.It must be in format YYYY-MM-DD and must be ended dates"
        # raise argparse.ArgumentTypeError(msg)
    return valid_date


def check_for_city_with_latitude_longitude(latitude, longitude, given_city):
    """This method checks the city with the latitude and longitude and validation of latitude and longitude"""
    try:
        geolocator = Nominatim(user_agent="geoapiExercises")
        if latitude and longitude:
            lat = str(latitude)
            long = str(longitude)
            location = geolocator.reverse(lat + "," + long)
            address = location.raw["address"]
            city = address.get("city", "")
            print(city)
            if city == given_city:
                datas["logger"].info(
                    "The given city %s matches with latitude and logitude city value %s",
                    given_city,
                    city,
                )
            else:
                datas["logger"].error(
                    "The given city %s did not match with city %s of given latitude and longitude are out of range",
                    given_city,
                    city,
                )
                print("City value didnot match or latitude or longitude is out of range")
                raise Exception
    except Exception as err:
        datas["logger"].error("Cannot get the details from api as there are some invalid details")
        print(err)
        city=None
        # sys.exit("System terminated for ivalid details")
    return city


def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser gets latitude,longitude,start,end dates to get data from sunrise sunset api"
    )
    parser.add_argument(
        "--latitude",
        help="Enter the latitude in format xx.xxxxxx for which details are needed",
        type=checkvalid_lat,
    )
    parser.add_argument(
        "--longitude",
        help="Enter the longitude in format xx.xxxxxx for which details are needed",
        type=checkvalid_long,
    )
    parser.add_argument("--city", help="Enter the city for which details are needed", type=str)
    parser.add_argument(
        "--startdate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
        default=current_date,
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
    )
    args = parser.parse_args()
    if args.latitude and args.longitude and args.city:
        datas["logger"].info(
            "Checks for the given city with given latitude, longitude and its ranges"
        )
        check_for_city_with_latitude_longitude(args.latitude, args.longitude, args.city)
    api_details = SunriseSunsetDataUploadS3(
        args.startdate, args.enddate, args.latitude, args.longitude, args.city
    )
    dates = api_details.get_details_for_givendates()
    api_details.get_lat_long_city_from_config(dates["date1"], dates["date2"])


if __name__ == "__main__":
    main()

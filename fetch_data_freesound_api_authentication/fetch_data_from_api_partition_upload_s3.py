"""This module fetches Similar sounds, user_packs from freesound api for the provided
sound id and username partition them based on the created date and upload them to s3
in the given path """

from datetime import datetime
import os
import argparse
from aws_s3.s3_details import S3Details
from logger_path.logger_object_path import LoggerPath
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api import PullDataFromFreeSoundApi

datas = LoggerPath.logger_object("freesound_api_datas")


class FetchDataFromApiUploadS3:
    """This class has methods to fetch datas from api based on the endpoints for the given
    sound id and username, partition them based on date and upload them to s3"""

    def __init__(self, soundid, username, endpoint):
        """This is the init method for the class HistoricDataProductsUploadS3"""
        self.logger = datas["logger"]
        self.soundid = soundid
        self.username = username
        self.endpoint = endpoint
        self.section = datas["config"]["freesound_api_datas"]
        self.s3_client = S3Details(self.logger, datas["config"])
        self.local = ApiDataPartitionUploadLocal(self.logger)
        self.api = PullDataFromFreeSoundApi(self.section, self.logger)
        self.logger.info("Successfully created the instance of the class")

    def fetch_response_from_api_for_endpoints(self):
        """This method fetches response from api for the given endpoints and convert to
        dataframe"""
        try:
            if self.endpoint == "similar_sound" and self.soundid:  # checks for endpoint
                self.logger.info(
                    "Fetching response for similar sounds from api for sound id %s", self.soundid
                )
                dataframe = self.api.fetch_similar_sounds_from_api(self.soundid)
                file_name = f"{self.soundid}.json"
                date = datetime.strftime(datetime.now().date(), "%Y-%m-%d")
                self.create_json_upload_s3(dataframe, file_name, date)
            elif self.endpoint == "user_packs" and self.username:
                self.logger.info(
                    "Fetching response for %s from api for username %s",
                    self.endpoint,
                    self.username,
                )
                dataframe = self.api.fetch_user_sounds_packs_from_api(self.username)
                date = self.get_date_from_dataframe(dataframe)
            else:
                self.logger.error("The given endpoint doesn't match with the required endpoints")
                # sys.exit("System terminated for invalid endpoints/soundid/username")
                dataframe = None
        except Exception as err:
            self.logger.error("Cannot get the response from api %s", err)
            dataframe = None
        return dataframe

    def get_date_from_dataframe(self, dataframe):
        """This method create json file and make partition based on the created date
        from dataframe"""
        try:
            date_created = list(dataframe.created)
            print(date_created)
            for date in date_created:
                new_df = dataframe[dataframe.created == date]
                if not new_df.empty:
                    file_name = f"{self.username}_{(date.split('T')[0])}.json"
                    self.create_json_upload_s3(new_df, file_name, date.split("T")[0])
        except Exception as err:
            new_df = None
            self.logger.error("Cannot get the date from the dataframe %s", err)
        return new_df

    def create_json_upload_s3(self, df_data, file_name, date):
        """This method creates the json file for the dataframe and upload to s3"""
        try:
            self.logger.info("Got the dataframe for the endpoint %s", self.endpoint)
            s3_path = self.section["s3_path"]
            local_path = LoggerPath.local_download_path("freesound_api_datas")
            partition_path = self.put_partition_path(date)
            df_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info("Created the json file for %s on %s", self.endpoint, date)
            copy_source = local_path + "/" + file_name
            self.logger.info("Uploading the json file to local s3 path")
            file = self.local.upload_partition_s3_local(
                local_path,
                copy_source,
                file_name,
                s3_path + "/" + self.endpoint + "/" + partition_path,
            )
            # file=self.upload_to_s3(s3_path,copy_source,s3_path+"/"+self.endpoint+"/"+partition_path, file_name)
        except Exception as err:
            self.logger.error("Cannot create a json file for %s on %s", self.endpoint, date)
            print("Canot create a json file and upload to s3 local", err)
            file = None
        return file

    def put_partition_path(self, date):
        """This method will make partion path based on year,month,date
        and avoid overwrite of file and upload to local
        parameters: s3_path- s3 path to store the datas
                    date - the date for which datas are needed"""
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d")
            self.logger.info("Made the partition based on date %s", partition_path)
        except Exception as err:
            self.logger.error("Cannot made a partition %s", err)
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on endpoint and date"""
        try:
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            file = self.s3_client.upload_file(source, self.section["bucket"], bucket_path, key)
            print("the file has been uploaded to s3 in the given path")
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file


def main():
    """This is the main method for the class"""
    parser = argparse.ArgumentParser(
        description="This argparser gets soundid,endpoint,username for fetching the datas from api"
    )
    parser.add_argument(
        "--soundid",
        help="Enter the sound id to fetch similar sounds from api",
        type=int,
    )
    parser.add_argument(
        "--username",
        help="Enter the username to fetch user packs and user sounds from api",
        type=str,
    )
    parser.add_argument(
        "endpoint",
        choices=["similar_sound", "user_packs"],
        help="Choose the endpoint from the choice for which datas to be fetched from api",
    )
    args = parser.parse_args()
    api_data = FetchDataFromApiUploadS3(args.soundid, args.username, args.endpoint)
    api_data.fetch_response_from_api_for_endpoints()


if __name__ == "__main__":
    main()

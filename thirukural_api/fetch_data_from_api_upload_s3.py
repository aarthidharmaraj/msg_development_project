"""This module gets the data of the kural from thirukural api for the given kural number,partition
them based on section, and upload to s3"""
from datetime import timedelta
import os
import argparse
import sys
import itertools
import pandas as pd
from logger_path.logger_object_path import LoggerPath
from aws_s3.s3_details import S3Details
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api import ThirukuralApi

datas = LoggerPath.logger_object("thirukural_api")

class ThirukuralDataUploadS3:
    """This class has methods to get thirukural data from api for the given kural number
    ,partition them and upload to s3"""

    def __init__(self):
        """This is the init method for the class ThirukuralDataUploadS3"""
        self.section = datas["config"]["thirukural_api"]
        self.logger = datas["logger"]
        self.logger.info("Successfully created the instance of the class")

    def get_response_from_api(self, kural_num):
        """This method fetches the data of thirukural from api based on the kural number given
        parameters : kural_num - the number for the kural for which datas are needed"""
        try:
            api = ThirukuralApi(self.section, self.logger)
            for kural_num in kural_num:
                self.logger.info("Getting response from api for kural %s", kural_num)
                print("\nFor kural number ",kural_num)
                response = api.get_endpoint_for_thirukural_data(kural_num)
                self.get_dataframe_for_response(response, kural_num)
        except Exception as err:
            self.logger.error(
                "Cannot able to get response from api for the kural_num %s - %s", kural_num, err
            )
            response = None
            print(err)
            sys.exit("System has terminated for fail in getting response from api")
        return response

    def get_dataframe_for_response(self, response, kural_num):
        """This method gets response from api,convert to dataframe and create json file for that
        parameters : response - response from api
                     kural_num - the kural_num for which datas are fetched"""
        try:
            if response is not None:
                self.logger.info("Got the response from api for %s", kural_num)
                df_data = pd.DataFrame.from_dict(response, orient="index")
                df_data = df_data.transpose()
                if not df_data.empty:
                    partition_path = self.put_partition_path(kural_num, response)
                    file_name = f"data_of_kural_{kural_num}.json"
                    self.create_json_file_partition(df_data, file_name, partition_path, kural_num)
            else:
                self.logger.info("No responses from api for the kural %s", kural_num)
                df_data = None
        except Exception as err:
            self.logger.error("Cannot create the dataframe for the given response %s", err)
            df_data = None
        return df_data

    def create_json_file_partition(self, df_data, file_name, partition_path, kural_num):
        """This method creates a temporary json file,create partition path and upload to s3
        parameters : df_data - dataframe created from the response
                    file_name - file_name to be uploaded in s3
                    partition_path - partition based on obj_id"""
        try:
            self.logger.info("Got the dataframe for the object id %s", kural_num)
            s3_path = self.section["s3_path"]
            temp_s3 = ApiDataPartitionUploadLocal(self.logger)
            local_path = LoggerPath.local_download_path("thirukural_datas")
            df_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                force_ascii=False,
                lines=True,
            )
            self.logger.info("Created the json file for %s", kural_num)
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

    def put_partition_path(self, kural_num, response):
        """This method will make partion path based on year,month and kural_num
        and avoid overwrite of file and upload to local"""
        try:
            sect = response["sect_eng"].replace(" ","_")
            chapgrp = response["chapgrp_eng"].replace(" ","_")
            chap = response["chap_eng"].replace(" ","_")
            partition_path = f"pt_section={sect}/pt_chaptergroup={chapgrp}/pt_chapter={chap}/pt_number={kural_num}"
            self.logger.info("Created the partition path for the given %s", kural_num)
        except Exception as err:
            self.logger.error("Cannot made a partition for %s because of %s", kural_num, err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on endpoint and kural_num"""
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


def check_valid_kuralnumber(kural_num):
    """This method checks for the valid kural_num entered"""
    try:
        kural_num = int(kural_num)
        if 1 <= kural_num <= 1330:  # checks the kural number given
            valid_kural = kural_num
            datas["logger"].info("The given kural_num is a valid kural_num %s", valid_kural)
        else:
            raise Exception
    except (Exception, ValueError):
        datas["logger"].info(
            "%s not valid kural_num.It should be in between 1 and 1330 of exixting kurals",
            kural_num,
        )
        valid_kural = None
        msg = f"{kural_num} not valid.It should be in between 1 and 1330 of exixting kurals"
        raise argparse.ArgumentTypeError(msg)
    return valid_kural


def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser is to get the kural number to get data from Thirukural api"
    )
    parser.add_argument(
        "--kural_num",
        help="The kural_num should be one among the existing kurals",
        type=check_valid_kuralnumber,
        nargs="*",
    )
    parser.add_argument("--startnum",help="Give the start range for kural details",type=check_valid_kuralnumber)
    parser.add_argument("--endnum",help="Give the end range for kural details",type=check_valid_kuralnumber)
    args = parser.parse_args()
    api_details = ThirukuralDataUploadS3()
    if args.kural_num:
        api_details.get_response_from_api(args.kural_num)
    elif args.startnum and args.endnum:
        count = (args.endnum - args.startnum)+1
        num=[]
        for i in range(count):  # Fetch for the given range of numbers one by one
            number = args.startnum +i
            num.append(number) 
        api_details.get_response_from_api(num)
    else:
        datas["logger"].error("Provide the Kural numbers or the range by giving start and end numbers")
        print("Provide the Kural numbers or the range by giving start and end numbers")


if __name__ == "__main__":
    main()

""" This module is to move the HostAspire files from sftp to S3, store the 
zip file in source path and extracted text file from stage path and make 
partition based on date and upload to s3"""

import logging
import configparser
import os
import argparse
from datetime import datetime
from zipfile import ZipFile
# from aws_s3.s3_details import S3Details
# from sftp.sftp_connection import SftpConnection
from sftp_to_s3_upload_local import MoveSftpToS3Local

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")

class MoveAspSftpToS3:
    """"This class has methods to move aspire files from sftp to s3
    make partition and store the extracted text file in the given stage path"""
    
    def __init__(self,logger_obj,startdate,enddate):
        """This is the init method for the class"""
        # self.sftp=SftpConnection(logger_obj,config)
        self.logger=logger_obj
        self.sftp_local=MoveSftpToS3Local(logger_obj,config)
        # self.s3_client=S3Details(logger_obj)
        self.startdate=startdate
        self.enddate=enddate
        self.local_path = os.path.join(
            parent_dir, config["local"]["data_path"], "sftp_to_s3_data"
        )
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)
        self.sftp_path = config["move_asp_from_sftp_to_s3"]["sftp_path"]
    
    def get_zip_file_from_sftp(self):
        """This method get the list of files from sftp"""
        
        try:
            # sftp_file_list = self.sftp_conn.list_files()
            sftp_file_list = self.sftp_local.list_files_sftp_local()
            for file_name in sftp_file_list:
                print(file_name)
                zip_source = self.sftp_path + file_name
                partition_path=self.put_partition_path(file_name)
                zip_upload=self.sftp_local.upload_parition_s3_local(self.local_path,zip_source,file_name,partition_path)
                text_source=self.extract_the_files_from_zip(zip_source)
                # file=self.sftp_local.upload_parition_s3_local(self.local_path,text_source,file_name,partition_path)
                # file=self.upload_to_s3(text_source,partition_path,file_name)
                file=None
        except Exception as err:
            self.logger.error(f"Cannot get the file from the sftp path {err}")
            print("Cannot get the file from given sftp path",err)
            file=None
        return file
    
    def extract_the_files_from_zip(self,zip_source):
        """This method extracts the file from zip folder"""
        s3path_stage = self.local_path+"/"+config["move_asp_from_sftp_to_s3"]["s3_path_stage"]
        try:
            with ZipFile(zip_source, 'r') as zipObj:
                zipObj.extractall(s3path_stage)
            text_source=s3path_stage
            print(text_source)
        except Exception as err:
            self.logger.error(f"Cannot extract the files from zip folder{err}")
            print("Cannot extract the files from zip folder")
            text_source=None
        return text_source
        
    def put_partition_path(self,file_name):
        """This method will make partion path based on year,
        month,date and upload to local"""
        try:
            partition_path = f"pt_year={file_name[4:8]}/pt_month={file_name[8:10]}/pt_day={file_name[10:12]}"
            print(partition_path)
        except Exception as err:
            self.logger.error(f"Cannot made a partition becausee of {err}")
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path
    
    def upload_to_s3(self, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        key = partition_path + "/" + file_name
        self.logger.info(f"The file is {file_name} being uploaded to s3 in the given path")
        print("the file has been uploaded to s3 in the given path")
        self.s3_client.upload_file(source, key)
        os.remove(source)
    
    
def valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        date= datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        date=None
        msg = "not a valid date: {0!r}".format(date)
        raise argparse.ArgumentTypeError(msg)
    return date

def main():
    """This is the main method for this module"""
    log_dir = os.path.join(parent_dir, config["local"]["log_path"], "move_sftp_to_s3")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "sftp_to_s3_logfile.log")

    logging.basicConfig(
        level=logging.INFO,
        filename=log_file,
        datefmt="%d-%b-%y %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        filemode="w",
    )
    logger_obj = logging.getLogger()
    parser = argparse.ArgumentParser(
        description="This argparser is to get the required date_of_joining need to filter"
    )
    parser.add_argument(
        "--startdate",
        help="Enter the start date in this format YYYY-MM-DD",
        type=valid_date,
    )
    parser.add_argument("--enddate", help="Enter the end date in this format YYYY-MM-DD",type=valid_date)
    
    args = parser.parse_args()
    move_file= MoveAspSftpToS3(
        logger_obj, args.startdate, args.enddate
    )
    move_file.get_zip_file_from_sftp()
    

if __name__ == "__main__":
    main()

"""This module is to move the HostAspire files from sftp to S3, store the 
zip file in source path and extracted text file from stage path and make 
partition based on date and upload to s3"""


from datetime import datetime
import os
import shutil
from zipfile import ZipFile
import sys
from aws_s3.s3_details import S3Details
from sftp.sftp_connection import SftpConnection
from logger_object_path import LoggerPath
from sftp_to_s3_upload_local import MoveSftpToS3Local

datas = LoggerPath.logger_object("load_sftp_to_s3")


class MoveAspSftpToS3:
    """ "This class has methods to move aspire files from sftp to s3
    make partition and store the extracted text file in the given stage path"""

    def __init__(self):
        """This is the init method for the class"""
        self.logger = datas["logger"]
        self.sftp_s3_local = MoveSftpToS3Local(self.logger)
        self.sftp_conn = SftpConnection(self.logger, datas["config"])
        self.s3_client = S3Details(self.logger, datas["config"])
        self.local_path = LoggerPath.local_download_path("sftp_to_s3_data")
        self.sftp_path = datas["config"]["move_asp_from_sftp_to_s3"]["sftp_path"]
        self.s3path_source = datas["config"]["move_asp_from_sftp_to_s3"]["s3_path_source"]
        self.s3path_stage = datas["config"]["move_asp_from_sftp_to_s3"]["s3_path_stage"]
        self.bucket = datas["config"]["move_asp_from_sftp_to_s3"]["bucket"]

    def get_zip_file_from_sftp(self):
        """This method get the list of files from sftp server"""
        try:
            sftp_file_list = self.sftp_s3_local.get_latest_files_from_local(
                self.local_path, self.s3path_source, self.sftp_path
            )
            # sftp_file_list = self.get_list_of_file_notin_s3_from_sftp()
            #if there is presence of new files in sftp iterate through each files if it is zip
            if sftp_file_list:
                for file_name in sftp_file_list:
                    if file_name.endswith('.zip'):
                        print(file_name)
                        zip_source = self.sftp_path + file_name
                        self.logger.info("Got the file %s from sftp", file_name)
                        self.get_partition_and_upload_files(file_name,zip_source)
                    else:
                        self.logger.info("The file format %s is not suported to the requirement",file_name)
                        print("The file format is not suported to the requirement",file_name)
                        zip_source=None
            else:
                zip_source = None
                self.logger.info("System terminated as there are no newly uploaded files in sftp")
                sys.exit("System terminated as there are no newly uploaded files in sftp")
        except Exception as err:
            self.logger.error("Cannot get the file from the sftp path %s", err)
            print("Cannot get the file from given sftp path", err)
            zip_source = None
        return zip_source
    
    def get_partition_and_upload_files(self,file_name,zip_source):
        """This method gets the partition path and upload the zip file to 
        source path and extract text file and upload to stage path."""
        try:
            temp_zip_source = self.local_path + "/" + file_name
            shutil.copy(zip_source, self.local_path + "/")
            base_file = file_name.split(".")[0]
            partition_path = self.put_partition_path(base_file)
            self.sftp_s3_local.upload_zip_text_file_to_given_path(
                                self.local_path,
                                partition_path,
                                temp_zip_source,
                                file_name,
                                self.s3path_source,
                            )
            self.logger.info("Successfully uploaded Zip files to the source path")
            temp_text_source = self.extract_the_files_from_zip(zip_source, base_file)
            file=self.sftp_s3_local.upload_zip_text_file_to_given_path(
                                self.local_path,
                                partition_path,
                                temp_text_source,
                                base_file + ".txt",
                                self.s3path_stage,
                        )
            self.logger.info("Successfully uploaded text files to the stage path")
            # file=self.fetch_file_from_sftp_upload_s3(file_name, temp_zip_source,partition_path)
        except Exception as err:
            self.logger.error("The file cannot be uploaded to the given path %s ",err)
            file=None
        return file
            

    def extract_the_files_from_zip(self, zip_source, base_file):
        """This method extracts the file from zip folder"""
        try:
            with ZipFile(zip_source, "r") as zipobj:
                zipobj.extractall(self.local_path)
            text_source = self.local_path + "/" + base_file + ".txt"
            self.logger.info("Extracted the text file from zip source")
        except Exception as err:
            self.logger.error("Cannot extract the files from zip folder%s", err)
            print("Cannot extract the files from zip folder")
            text_source = None
        return text_source

    def put_partition_path(self, file_name):
        """This method will make partion path based on year,
        month,date and upload to local"""
        try:
            date_obj = datetime.strptime(file_name, "ASP_%Y%m%d").date()
            partition_path = date_obj.strftime("pt_year=%Y/pt_month=%m/pt_day=%d")
            print(partition_path)
        except Exception as err:
            self.logger.error("Cannot made a partition becausee of %s", err)
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def get_list_of_file_notin_s3_from_sftp(self):
        """This method get the list of files from s3 and compare with
        sftp files for newly uploaded"""
        try:
            uploaded_file_list = self.s3_client.get_list_of_files_in_s3(self.s3path_source)
            self.logger.info("Got the list of files from s3 %s", uploaded_file_list)
            sftp_file_list = self.sftp_conn.list_files(self.sftp_path)
            self.logger.info("Got the list of files from sftp %s", sftp_file_list)
            new_file_list = list(set(sftp_file_list).difference(set(uploaded_file_list)))
            self.logger.info(
                "Created file_list %s containing newly uploaded files from sftp compared with s3",
                new_file_list,
            )
        except Exception as err:
            self.logger.error("Cannot get the newly uploaded files from sftp %s", err)
            new_file_list = None
        return new_file_list

    def fetch_file_from_sftp_upload_s3(self, file, zip_source, partition_path, base_file):
        """This method get file from sftp without downloading to local file and upload to s3"""
        try:
            with self.sftp_conn.open(self.sftp_path + file, "r") as file_name:
                file_name.prefetch()
                self.logger.info(
                    "The file %s has been fetched from sftp and passed upload method in s3",
                    file_name,
                )
                self.upload_file_to_s3(zip_source, self.s3path_source, partition_path, file_name)
                self.logger.info(
                    "Uploaded the zip file %s from sftp server to s3 in the given source path",
                    file_name,
                )
            text_source = self.extract_the_files_from_zip(zip_source, file_name)
            file = self.upload_file_to_s3(
                text_source, self.s3path_stage, partition_path, base_file + ".txt"
            )
            self.logger.info("Uploaded the extracted text file %s to s3", base_file + ".txt")
        except IOError as ier:
            print("The file cannot be opened or uploaded to s3 from sftp", ier)
            self.logger.error("The file cannot be opened or uploaded to s3 from sftp %s", ier)
            file = None
        return file

    def upload_file_to_s3(self, source, bucket_path, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        try:
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            print("the file has been uploaded to s3 in the given path")
            file = self.s3_client.upload_file(source, self.bucket, bucket_path, key)
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file


def main():
    """This is the main method for this module"""
    load_file = MoveAspSftpToS3()
    load_file.get_zip_file_from_sftp()


if __name__ == "__main__":
    main()

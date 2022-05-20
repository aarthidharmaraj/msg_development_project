""" This module is to move the HostAspire files from sftp to S3, store the 
zip file in source path and extracted text file from stage path and make 
partition based on date and upload to s3"""


import os
import shutil
from zipfile import ZipFile

from numpy import partition
# from aws_s3.s3_details import S3Details
# from sftp.sftp_connection import SftpConnection
from logger_object_path import LoggerPath
from sftp_to_s3_upload_local import MoveSftpToS3Local

datas = LoggerPath.logger_object()
class MoveAspSftpToS3:
    """"This class has methods to move aspire files from sftp to s3
    make partition and store the extracted text file in the given stage path"""
    
    def __init__(self):
        """This is the init method for the class"""
        self.logger=datas["logger"]
        self.sftp_local=MoveSftpToS3Local(self.logger,datas['config'])
         # self.sftp_conn=SftpConnection(self.logger,datas['config'])
        # self.s3_client=S3Details(self.logger)
        self.local_path = os.path.join(
            datas['parent_dir'], datas['config']["local"]["data_path"], "sftp_to_s3_data"
        )
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)
        self.sftp_path =  datas['config']["move_asp_from_sftp_to_s3"]["sftp_path"]
        self.s3path_source =  datas['config']["move_asp_from_sftp_to_s3"]["s3_path_source"]
        self.s3path_stage = datas['config']["move_asp_from_sftp_to_s3"]["s3_path_stage"]
        
    def get_zip_file_from_sftp(self):
        """This method get the list of files from sftp server"""
        
        try:
            # sftp_file_list = self.sftp_conn.list_files()
            sftp_file_list = self.sftp_local.list_files_sftp_local()
            for file_name in sftp_file_list:
                # print(file_name)
                zip_source = self.sftp_path + file_name
                self.logger.info(f"Got the file from sftp")
                self.upload_zip_file_to_source_path(zip_source,file_name)
                # self.get_file_from_sftp_upload_s3(file_name)
        except Exception as err:
            self.logger.error(f"Cannot get the file from the sftp path {err}")
            print("Cannot get the file from given sftp path",err)
            zip_source=None
        return zip_source
    
    def upload_zip_file_to_source_path(self,zip_source,file_name):
        """This method uploads the zip file to the given source path"""
        try:
            partition_path=self.put_partition_path(file_name) 
            temp_zip_source= self.local_path + "/" +file_name
            shutil.copy(zip_source, self.local_path + "/")
            
            self.logger.info("Uploading the Zip file from sftp server to the source path" )
            file=self.sftp_local.upload_parition_s3_local(self.local_path+"/"+self.s3path_source,temp_zip_source,file_name,partition_path)
            # file=self.upload_file_to_s3(temp_zip_source,partition_path,file_name)
            self.upload_text_file_to_stage_path(zip_source,file_name)
        except Exception as err:
            self.logger.error(f"Cannot upload the zip file to the source path")
            file=None
        return file
    
    def upload_text_file_to_stage_path(self,zip_source,file_name):
        """This method uploads the extracted text file and upload to the stage path"""
        try:
            temp_text_source=self.extract_the_files_from_zip(zip_source,file_name)
            print(temp_text_source)
            partition_path=self.put_partition_path(file_name) 
            print(partition_path)
            file=self.sftp_local.upload_parition_s3_local(self.local_path+"/"+self.s3path_stage,temp_text_source,file_name[0:12]+".txt",partition_path)
            # # file=self.upload_file_to_s3(temp_text_source,partition_path,file_name[0:12]+".txt")
        except Exception as err:
            file=None
            self.logger.error(f"Cannot upload the text file to the given stage path{err}")
        return file
            
    def extract_the_files_from_zip(self,zip_source,file_name):
        """This method extracts the file from zip folder"""
        try:
            with ZipFile(zip_source, 'r') as zipObj:
                zipObj.extractall(self.local_path)
            text_source=self.local_path+"/"+file_name[0:12]+".txt"
            self.logger.info(f"Extracted the text file from zip source {zipObj}")
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
            # print(partition_path)
        except Exception as err:
            self.logger.error(f"Cannot made a partition becausee of {err}")
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path
    
    def get_file_from_sftp_upload_s3(self, file,zip_source):
        """This method get file from sftp without downloading to local file and upload to s3"""
        try:
            partition_path=self.put_partition_path(self,file)
            with self.sftp_conn.open(self.sftp_path + file, "r") as file_name:
                file_name.prefetch()
                self.logger.info(
                    "The file has been fetched from sftp and passed upload method in s3"
                )
                self.upload_file_to_s3(zip_source,self.s3path_source+"/"+partition_path,file_name)
                self.logger.info("Uploading the zip file from sftp server to s3 in the given source path")
                text_source=self.extract_the_files_from_zip(zip_source,file_name)
                self.upload_file_to_s3(text_source,self.s3path_stage+"/"+partition_path,file_name)
        except IOError as ier:
            print("The file cannot be opened in sftp", ier)
            self.logger.error("The file cannot be opened in sftp")
            
    def upload_file_to_s3(self, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        key = partition_path + "/" + file_name
        self.logger.info(f"The file is {file_name} being uploaded to s3 in the given path")
        print("the file has been uploaded to s3 in the given path")
        self.s3_client.upload_file(source, key)
        os.remove(source)

def main():
    """This is the main method for this module"""
    load_file= MoveAspSftpToS3()
    load_file.get_zip_file_from_sftp()
    
if __name__ == "__main__":
    main()

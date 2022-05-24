"""This module moves the files from sftp to s3 in local"""
import os
import shutil


class MoveSftpToS3Local:
    """This class has the methods that do in local folder for sftp and s3"""

    def __init__(self, logger, config):
        self.logger = logger
        # self.sftp_path = config["move_asp_from_sftp_to_s3"]["sftp_path"]

    def list_files_sftp_local(self,sftp_path):
        """This method gets the list of files names for the given local sftp path by filtering"""
        try:
            sftp_file_list =list(os.listdir(sftp_path))
            self.logger.info("Received the list of files from sftp local")
        except Exception as err:
            sftp_file_list = None
            self.logger.error("Cannot get the files from sftp local %s", err)
            print("Cannot get the files from local sftp", err)
        return sftp_file_list

    def get_latest_files_from_local(self, local_path, source_path,sftp_path):
        """This method gets the latest files from local sftp compared with local s3"""
        try:
            temp_s3_files = []
            s3_file_path = local_path + "/" + source_path
            for (root_path,dirs,files) in os.walk(s3_file_path):
                for file in files:
                    temp_s3_files.append(file)
            self.logger.info("Got the files %s from source path of local s3", temp_s3_files)
            temp_sftp_files = self.list_files_sftp_local(sftp_path)
            self.logger.info("Got the files %s from local sftp", temp_sftp_files)
            latest_files = list(set(temp_s3_files).symmetric_difference(set(temp_sftp_files)))
            print("Newly uploaded files", latest_files)
            self.logger.info("Got the newly uploaded files %s from local s3", latest_files)
        except Exception as err:
            self.logger.error(f"Cannot get the latest file from local sftp {err}")
            latest_files = None
        return latest_files

    def upload_partition_s3_local(self, local_path, copy_source, file_name, partition_path):
        """This method uploads weatherdata in the partition path in the form of json"""
        try:
            new_dir = local_path + "/" + partition_path + "/"
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
            shutil.copy(copy_source, new_dir)
            os.remove(copy_source)
            print(f"Successfully uploaded file'{file_name}' in the given path'{partition_path}'\n")
            self.logger.info(
                "Successfully uploaded file %s in the given path %s", file_name, partition_path
            )

        except Exception as err:
            print("Cannot upload the  file in the given path:", err)
            self.logger.error(f"Cannot upload the file in the given path{err}")
            file_name = None
        return file_name

"""This module has methods to upload the files in the local path with the partition path"""
import os
import shutil


class ApiDataPartitionUploadLocal:
    """This class has the methods that do in local folder for sftp and s3"""

    def __init__(self, logger):
        self.logger = logger

    def upload_partition_s3_local(self, local_path, copy_source, file_name, partition_path):
        """This method uploads given file in the partition path in local s3"""
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

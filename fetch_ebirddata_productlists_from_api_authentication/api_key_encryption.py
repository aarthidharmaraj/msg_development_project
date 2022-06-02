"""This module generates the fernet key , with that encrypt the given api key
and add them to the config file"""
from logger_path.logger_object_path import LoggerPath
from cryptography.fernet import Fernet

class EncryptDecryptApiKey:
    """This class has methods to generate the fernet key ,encrypt api key, update in config file"""

    def __init__(self):
        """This is the init method for the class EncryptApiKey"""
        datas = LoggerPath.logger_object("encryption_decryption")
        self.config = datas["config"]
        self.logger = datas["logger"]
        self.parent_dir = datas["parent_dir"]

    def encrypt_api_key(self):
        """This method encrypts the api key and store it in config file"""
        try:
            key = Fernet.generate_key().decode()
            self.logger.info("Generated the fernet key")
            self.config.set("eBird_api_datas",'fernet_key',key)
            fernet_key_config=self.config["eBird_api_datas"]["fernet_key"]
            fernet = Fernet(fernet_key_config.encode())
            api_key = "ghvcqquvmk97"
            encrypt_message = fernet.encrypt(api_key.encode()).decode()
            self.logger.info("Encrypted the given key token%s", encrypt_message)
            self.config.set("eBird_api_datas", "api_key", encrypt_message)
            with open(self.parent_dir + "/details.ini", "w", encoding="utf-8") as file:
                self.config.write(file)
            self.logger.info("Updated the api_key and fernet key in config file")
        except Exception as err:
            self.logger.error(
                "Cannot encrypt the given key tokenand store it in config file %s", err
            )
            encrypt_message = None
        return encrypt_message

obj=EncryptDecryptApiKey()
obj.encrypt_api_key()
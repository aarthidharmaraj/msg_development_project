"""This module encrypt the api key and store it in the config file as well
as decrypt the api key from config file"""

import base64
import rsa
from logger_path.logger_object_path import LoggerPath

publicKey, privateKey = rsa.newkeys(512)


class EncryptDecryptApiKey:
    """This class has methods to encrypt api key, update in config file
    and decrypt the api key from config file"""

    def __init__(self):
        """This is the init method for the class EncryptApiKey"""
        datas = LoggerPath.logger_object("encryption_decryption")
        self.config = datas["config"]
        self.logger = datas["logger"]
        self.parent_dir = datas["parent_dir"]

    def encrypt_api_key(self):
        """This method encrypts the api key and store it in config file"""
        try:
            message = "ghvcqquvmk97"
            encrypt_message = rsa.encrypt(message.encode(), publicKey)
            base64_text = base64.b64encode(encrypt_message).decode()
            self.logger.info("Encrypted the given message %s", base64_text)
            self.config.set("eBird_api_datas", "api_key", base64_text)
            with open(self.parent_dir + "/details.ini", "w", encoding="utf-8") as file:
                self.config.write(file)
            self.logger.info("Updated the api_key with the encrypted message in config file")
        except Exception as err:
            self.logger.error(
                "Cannot encrypt the given message and store it in config file %s", err
            )
            encrypt_message = None
        return encrypt_message

    def decrypt_from_config(self):
        """This method gets the encrypted api key from config, decrypt it and return the data"""
        try:
            self.encrypt_api_key()
            encrypt_key = self.config["eBird_api_datas"]["api_key"]
            self.logger.info("Got the encrypted key from config %s", encrypt_key)
            decrypt_message = (
                rsa.decrypt(base64.b64decode(encrypt_key.encode()), privateKey)
            ).decode()
            self.logger.info("Successfully decrypted the key %s", decrypt_message)
        except Exception as err:
            decrypt_message = None
            print(err)
            self.logger.error("Cannot decrypt the api key from config %s", err)
        return decrypt_message


# obj=EncryptDecryptApiKey()
# obj.decrypt_from_config()

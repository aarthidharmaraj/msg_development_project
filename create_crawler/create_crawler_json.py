"""This module had script for create crawler in aws from the json definition"""

import csv
import json
import boto3

glue_client = boto3.client("glue", region_name="ap-south-1")


def create_crawler_aws():
    """This method creates the crwaler in AWS for details in CSV file"""
    exceptioncrawler = []

    with open("sample_crawler_definition.json", "r", encoding="UTF-8") as file:
        data = json.load(file)
        try:
            dict_filter = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
            crawler_details = (
                "Name",
                "Role",
                "DatabaseName",
                "Description",
                "Targets",
                "Classifiers",
                "TablePrefix",
                "Schedule",
                "SchemaChangePolicy",
                "RecrawlPolicy",
                "LineageConfiguration",
                "LakeFormationConfiguration",
                "Configuration",
                "CrawlerSecurityConfiguration",
                "Tags",
            )
            matched = dict_filter(data, crawler_details)
            glue_client.create_crawler(**matched)
            print("Successfully created the crawler in aws")
        except Exception as excep:
            exceptioncrawler.extend([data["Name"], excep])
            print("\n Cannot be able to create the crawler in aws\n", exceptioncrawler)


class CreateCrawler:
    """This is the Crawler class in the module"""

    def __init__(self, crawler_name, database_name, s3_path, role):
        """This is the init method of class CreateCrawler"""
        self.crawler_name = crawler_name
        self.database_name = database_name
        self.s3_path = s3_path
        self.role = role

    def check_crawler_name(self):
        """This method checks if the crawler name need to update in s3 path of csv file"""
        with open("sample_crawler_definition.json", "r", encoding="utf-8") as file:
            data = json.load(file)
            if self.crawler_name in data["Name"]:
                print(
                    "\nThe crawler [",
                    self.crawler_name,
                    "] is present in crawler definiton,updating the details",
                )
                self.update()
            else:
                print(
                    "\nCannot create the crawler [",
                    self.crawler_name,
                    "] because of lack of definiton",
                )

    def update(self):
        """This method updates crawler details in crawler definition if all details are present"""
        with open("sample_crawler_definition.json", "r", encoding="utf-8") as file:
            data = json.load(file)
            if self.crawler_name and self.database_name and self.s3_path and self.role:
                if self.crawler_name == data["Name"]:

                    strrole = str(self.role)
                    strdatabase = str(self.database_name)
                    strpath = str(self.s3_path)
                    data["DatabaseName"] = strdatabase
                    data["Role"] = strrole
                    # data["Targets"]["S3Targets"][0]=strpath
                with open(
                    "sample_crawler_definition.json", "w", encoding="utf-8"
                ) as jsonfile:
                    json.dump(data, jsonfile)
            """else create the crawler with the existing definiton"""
        create_crawler_aws()


def main():
    """This is the main method"""
    with open("crawler_details.csv", "r", encoding="UTF-8") as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for datas in csv_reader:
            # print(datas['crawler_name'])
            crawl = CreateCrawler(
                datas["crawler_name"],
                datas["database_name"],
                datas["s3_path"],
                datas["role"],
            )
            crawl.check_crawler_name()
            # if datas['crawler_name'] and datas['database_name'] and datas['s3_path'] and datas['role']:
            #     crawl = CreateCrawler(
            #         datas['crawler_name'],datas['database_name'],datas['s3_path'],datas['role']
            #     )
            #     crawl.check_crawler_name()
            # else:
            #     print("The crawler [",datas['crawler_name'],"] has some missing details like database/s3_path/role")


if __name__ == "__main__":
    main()

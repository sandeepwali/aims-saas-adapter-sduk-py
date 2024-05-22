import requests
from requests.exceptions import HTTPError
import json
import os
from dotenv import load_dotenv
from modules.sduk.common import set_logger
from functools import lru_cache
from time import time

logger = set_logger("SaaS Api")

load_dotenv()


class AIMSSaaSAPIClient:
    BASE_URL = os.getenv("AIMS_SAAS_URL")

    def __init__(self):
        self.username = os.getenv("AIMS_SAAS_USERNAME", None)
        self.password = os.getenv("AIMS_SAAS_PASSWORD", None)
        self.access_token = None
        self.refresh_token = None
        self.company = os.getenv("AIMS_SAAS_COMPANY", None)

    @lru_cache(maxsize=2)
    def get_access_token(self, ttl_func=round(time() / 300)):
        del ttl_func
        """
        Retrieve the authentication token using the provided username and password.
        """
        endpoint = f"{self.BASE_URL}/api/v2/token"

        headers = {"accept": "application/json", "Content-Type": "application/json"}

        data = {"username": self.username, "password": self.password}

        response = requests.post(endpoint, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            self.access_token = response.json()["responseMessage"]["access_token"]
            self.refresh_token = response.json()["responseMessage"]["refresh_token"]
            return self.access_token
        else:
            response.raise_for_status()

    def get_article_upload_format(self):
        """
        Get article upload format
        """
        if not self.access_token:
            raise ValueError(
                "Token not set. Please authenticate before making requests."
            )

        endpoint = f"{self.BASE_URL}/api/v2/common/articles/upload/format"
        params = {"company": self.company}

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        response = requests.put(endpoint, headers=headers, params=params)
        if response.status_code in (200, 202):
            return response.json()
        else:
            # 401,403,405
            response.raise_for_status()

    def add_articles(self, store_code, articles, chunk_size=5000):
        """
        Add articles for specific company code
        """
        if not self.access_token:
            raise ValueError(
                "Token not set. Please authenticate before making requests."
            )

        endpoint = f"{self.BASE_URL}/api/v2/common/articles"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        params = {"company": self.company, "store": store_code}

        while articles:
            articles_chunk, articles = articles[:chunk_size], articles[chunk_size:]
            response = requests.put(
                endpoint, headers=headers, params=params, json=articles_chunk
            )
            if response.status_code not in (200, 202):
                # 401,403,405
                logger.error(response.json().get("responseMessage", ""))
                response.raise_for_status()
            else:
                logger.debug(
                    f"Sent {len(articles_chunk)} for store {store_code}, {len(articles)} pending"
                )

    def get_article(self, store_code, article_id):
        """
        Get article for specific company code
        """
        if not self.access_token:
            raise ValueError(
                "Token not set. Please authenticate before making requests."
            )

        endpoint = f"{self.BASE_URL}/api/v1/articles/article"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        params = {
            "company": self.company,
            "stationCode": store_code,
            "articleId": article_id,
        }

        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code not in (200, 202):
            # 401,403,405
            logger.error(response.json().get("responseMessage", ""))
            response.raise_for_status()
        else:
            logger.debug(f"Got {article_id} for store {store_code}")
            return response.json()

    def unlink_label(self, label_code):
        """
        Unlink a label for a given company and label code.
        """
        if not self.access_token:
            raise ValueError(
                "Token not set. Please authenticate before making requests."
            )

        endpoint = f"{self.BASE_URL}/api/v1/labels/unlink"
        params = {"company": self.company, "labelCode": label_code}

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        response = requests.post(endpoint, headers=headers, params=params)
        if response.status_code in (200, 202):
            return response.json()
        else:
            # 401,403,405
            response.raise_for_status()


def main():
    company = "SEG"
    label_code = "05F0A1B4B09C"
    # Example of how to use the API client:
    client = AIMSSaaSAPIClient()
    access_token = client.get_access_token()
    print(f"Retrieved Access Token: {access_token}")
    print(f"Retrieved Refresh Token: {client.refresh_token}")

    try:
        result = client.unlink_label(company=company, label_code=company)
        print(f"Unlink result: {result}")
    except HTTPError as http_err:
        print(
            f"HTTP error occurred while unlinking label {label_code} for company {company}. Error: {http_err}, Response: {http_err.response.text}"
        )
    except Exception as e:
        print(f"Error unlinking label {label_code} for company {company}. Error: {e}")


if __name__ == "__main__":
    main()

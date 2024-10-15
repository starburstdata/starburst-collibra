#import markdown
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError

GALAXY_API_REQUESTS_TIMEOUT=30


class GalaxyApiException(Exception):
    pass


class StarburstGalaxyService(object):

    def __init__(self, account_domain=None, client_id=None, client_secret=None):
        self.account_domain = account_domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = self.get_access_token()

    def get_access_token(self):
        access_token_url = f'https://{self.account_domain}/oauth/v2/token'
        try:
            access_token_response = requests.post(
                access_token_url,
                auth=HTTPBasicAuth(self.client_id, self.client_secret),
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                data={'grant_type': 'client_credentials'},
                timeout=GALAXY_API_REQUESTS_TIMEOUT
            )
            access_token_response.raise_for_status()
            return access_token_response.json()['access_token']
        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

    def get_data_product(self, data_product_id: str):
        api_url = f'https://{self.account_domain}/public/api/v1/dataProduct/{data_product_id}'
        try:
            response = requests.get(
                api_url,
                headers={
                    'Authorization': f'Bearer {self.access_token}'
                },
                timeout=GALAXY_API_REQUESTS_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

    def get_data_products(self, data_product_name=None):
        api_url = f'https://{self.account_domain}/public/api/v1/dataProduct'
        db_dict = {}

        try:
            response = requests.get(
                api_url,
                headers={
                    'Authorization': f'Bearer {self.access_token}'
                },
                timeout=GALAXY_API_REQUESTS_TIMEOUT
            )
            response.raise_for_status()

            dp_response = response.json()['result']
            print(dp_response)

            for data_product in dp_response:

                # If a data product name is provided as input, then filter the
                # list of data products by the name provided
                if data_product_name and data_product['name'] != data_product_name:
                    continue

                # Add an entry in the dictionary for the data product.  The ID of the data product will
                # be the key for the entry.  All other attributes of the data product will be a key/value dictionary
                # nested below the data product ID
                db_dict[data_product['dataProductId']] = {}

                # Add the name of the data product to the dictionary entry for the data product
                db_dict[data_product['dataProductId']]['name'] = data_product['name']

                # Add the creator, summary, catalog, schema, and last updated date for the data product
                # to the dictionary entry for the data product
                db_dict[data_product['dataProductId']]['createdBy'] = data_product['createdBy']['email']
                db_dict[data_product['dataProductId']]['summary'] = data_product['summary']
                db_dict[data_product['dataProductId']]['catalogName'] = data_product['catalog']['catalogName']
                db_dict[data_product['dataProductId']]['schemaName'] = data_product['schemaName']
                db_dict[data_product['dataProductId']]['lastUpdated'] = data_product['modifiedOn']

                db_dict[data_product['dataProductId']]['description'] = ''
                if 'description' in data_product:
                    db_dict[data_product['dataProductId']]['description'] = data_product['description']

                db_dict[data_product['dataProductId']]['productOwners'] = {}
                for contact in data_product['contacts']:
                    db_dict[data_product['dataProductId']]['productOwners'][contact['userId']] = contact['email']

                # TODO get SQL for views/mvs
                # to get view definition, will need to execute SHOW CREATE VIEW sql statement.

                datasets = self.get_table_info(data_product['catalog']['catalogId'], data_product['schemaName'])
                print(datasets)
                # Add a nested dictionary for the data product entry that contains the list of views
                # for the data product
                db_dict[data_product['dataProductId']]['views'] = {}
                db_dict[data_product['dataProductId']]['materialized_views'] = {}
                db_dict[data_product['dataProductId']]['tables'] = {}
                for dataset in datasets:
                    table_columns = self.get_table_columns(
                        data_product['catalog']['catalogId'],
                        data_product['schemaName'],
                        dataset['tableId']
                    )
                    if dataset['tableType'] == 'BASE TABLE':
                        db_dict[data_product['dataProductId']]['tables'][dataset['tableId']] = {}
                        db_dict[data_product['dataProductId']]['tables'][dataset['tableId']]['columns'] = {}
                        for column in table_columns:
                            db_dict[data_product['dataProductId']]['tables'][dataset['tableId']]['columns'][column['columnId']] = {}
                    elif dataset['tableType'] == 'VIEW':
                        db_dict[data_product['dataProductId']]['views'][dataset['tableId']] = {}
                        db_dict[data_product['dataProductId']]['views'][dataset['tableId']]['columns'] = {}
                        for column in table_columns:
                            db_dict[data_product['dataProductId']]['views'][dataset['tableId']]['columns'][column['columnId']] = {}
                        fq_view_name = f"{data_product['catalog']['catalogName']}.{data_product['catalog']['catalogName']}.{dataset['tableId']}"
                        # connect to cluster and run show create view
                    elif dataset['tableType'] == 'MATERIALIZED VIEW':
                        # MVs and views actually all come as tableType VIEW
                        db_dict[data_product['dataProductId']]['materialized_views'][dataset['tableId']] = {}
                    else:
                        print('this should not happen?')
                        print(dataset['tableType'])

            return db_dict
        except HTTPError as http_err:
            print(f'http err')
            print(db_dict)
            print(f'{http_err}')
            return None
        except Exception as err:
            print('some exception')
            print(db_dict)
            print(f'{err}')
            return None

    def get_table_info(self, catalog_id: str, schema_id: str):
        api_url = f'https://{self.account_domain}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table'
        try:
            response = requests.get(
                api_url,
                headers={
                    'Authorization': f'Bearer {self.access_token}'
                },
                timeout=GALAXY_API_REQUESTS_TIMEOUT
            )
            response.raise_for_status()
            return response.json()['result']
        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

    def get_table_columns(self, catalog_id: str, schema_id: str, table_id: str):
        api_url = f'https://{self.account_domain}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table/{table_id}/column'
        try:
            response = requests.get(
                api_url,
                headers={
                    'Authorization': f'Bearer {self.access_token}'
                },
                timeout=GALAXY_API_REQUESTS_TIMEOUT
            )
            response.raise_for_status()
            return response.json()['result']
        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

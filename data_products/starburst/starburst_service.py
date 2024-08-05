#@import markdown
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError


class StarburstService(object):

    def __init__(self, sep_url=None, sep_user=None, sep_role=None, sep_pwd=None):
        self.sep_url = sep_url
        self.sep_user = sep_user
        self.sep_role = sep_role
        self.sep_pwd = sep_pwd

    # Query for all data domains in Starburst
    def get_data_domains(self):
        # Set URL and headers for "Get All Data Domains" API call
        api_url = self.sep_url + "/api/v1/dataProduct/domains/"
        if self.sep_role:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Trino-Role': self.sep_role}
        else:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        try:
            # Issue API call to get all data domains from Starburst
            response = requests.get(api_url, headers=headers, auth=HTTPBasicAuth(self.sep_user, self.sep_pwd))
            response.raise_for_status()

            return response.json()

        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

    # Query for all tags assigned to a data product in Starburst using the ID of the data product
    def get_data_product_tags(self, data_product_id: str):
        # Set URL and headers for "Get Data Product Tags by Data Product ID" API call
        api_url = self.sep_url + f"/api/v1/dataProduct/tags/products/{data_product_id}"
        if self.sep_role:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Trino-Role': self.sep_role}
        else:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        try:
            # Issue API call to get all tags for the specified data product
            response = requests.get(api_url, headers=headers, auth=HTTPBasicAuth(self.sep_user, self.sep_pwd))
            response.raise_for_status()

            return response.json()

        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

    # Get data products by data product ID to get the full definition i.e. views, materialized views
    # This call is required to get the full definition because the "Get All Data Products" API call
    # in the get_data_products() function does not return the views/materialized views for a data product
    def get_data_product_by_id(self, data_product_id: str):
        api_url = self.sep_url + f"/api/v1/dataProduct/products/{data_product_id}"
        if self.sep_role:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Trino-Role': self.sep_role}
        else:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        try:
            # Issue the API call to get the full definition of the specified data product (by data product ID)
            response = requests.get(api_url, headers=headers, auth=HTTPBasicAuth(self.sep_user, self.sep_pwd))
            response.raise_for_status()

            return response.json()

        except HTTPError as http_err:
            print(f'{http_err}')
            return None
        except Exception as err:
            print(f'{err}')
            return None

    # Query for all "published" data products in Starburst
    def get_data_products(self, data_product_name=None):
        # Set URL and headers for "Get All Data Products" API call
        api_url = self.sep_url + "/api/v1/dataProduct/products/"
        if self.sep_role:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'X-Trino-Role': self.sep_role}
        else:
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        db_dict = {}

        # Call the function to get all data domain IDs and names.  The "Get All Data Products"
        # API call only returns domain IDs for each data product, which will be replaced with
        # the more descriptive domain name when published to Collibra
        domain_list = self.get_data_domains()

        try:
            # Issue the API call to get all data products from Starburst
            response = requests.get(api_url, headers=headers, auth=HTTPBasicAuth(self.sep_user, self.sep_pwd))
            response.raise_for_status()

            dp_response = response.json()

            # Loop through each data product returned by the "Get All Data Products" API call
            for data_product in dp_response:

                # If a data product name is provided as input, then filter the
                # list of data products by the name provided
                if data_product_name and data_product['name'] != data_product_name:
                    continue

                # Filter out any data products that are not "published"
                if data_product['status'] != "PUBLISHED":
                    continue

                # Add an entry in the dictionary for the data product.  The ID of the data product will
                # be the key for the entry.  All other attributes of the data product will be a key/value dictionary
                # nested below the data product ID
                db_dict[data_product['id']] = {}

                # Add the name of the data product to the dictionary entry for the data product
                db_dict[data_product['id']]['name'] = data_product['name']

                # Loop through the list of data domains to pull the name for the domain
                # using the ID of the domain.  Add the name of the domain to the dictionary
                # entry for the data product.
                for domain in domain_list:
                    if domain['id'] == data_product['dataDomainId']:
                        db_dict[data_product['id']]['domainName'] = domain['name']

                # Add the creator, summary, catalog, schema, and last updated date for the data product
                # to the dictionary entry for the data product
                db_dict[data_product['id']]['createdBy'] = data_product['createdBy']
                db_dict[data_product['id']]['summary'] = data_product['summary']
                db_dict[data_product['id']]['catalogName'] = data_product['catalogName']
                db_dict[data_product['id']]['schemaName'] = data_product['schemaName']
                db_dict[data_product['id']]['lastUpdated'] = data_product['updatedAt']

                # Add the description for the data product to the dictionary entry for the data product
                # and preserve any markdown that was used in this field
                if 'description' in data_product:
                    md_string = '''{description}'''.format(description=data_product['description'])
                    html_string = markdown.markdown(md_string, extensions=['sane_lists'])
                    db_dict[data_product['id']]['description'] = html_string.replace("\n", "<br>")

                # Get the details for a specific data product (using the data product's ID) to get the list
                # of views and/or materialized views for the data product
                dp_response = self.get_data_product_by_id(data_product_id=data_product['id'])
                if dp_response:
                    # If the data product has owners, add a nested dictionary to list out the owners
                    if dp_response.__contains__('owners'):
                        db_dict[data_product['id']]['productOwners'] = {}
                        # For each owner of the data product, add a new entry in the nested dictionary
                        # The 'key' is the owners name and the 'value' is the owners email
                        for owner in dp_response['owners']:
                            if owner.__contains__('name') and owner.__contains__('email'):
                                db_dict[data_product['id']]['productOwners'][owner['name']] = owner['email']

                    # Add a nested dictionary for the data product entry that contains the list of views
                    # for the data product
                    db_dict[data_product['id']]['views'] = {}

                    # For each view, add the name, description and SQL used to create the view
                    for view in dp_response['views']:
                        db_dict[data_product['id']]['views'][view['name']] = {}
                        if view.__contains__('definitionQuery'):
                            db_dict[data_product['id']]['views'][view['name']]['sql'] = view['definitionQuery']
                        if view.__contains__('description'):
                            db_dict[data_product['id']]['views'][view['name']]['description'] = view['description']

                        # Add a nested dictionary for the view in the data product entry that contains the list
                        # of columns for that view
                        db_dict[data_product['id']]['views'][view['name']]['columns'] = {}

                        # For each column in the view, add the name and description
                        for column in view['columns']:
                            if column.__contains__('description'):
                                db_dict[data_product['id']]['views'][view['name']]['columns'][column['name']] = column[
                                    'description']  # TODO

                    # Add a nested dictionary for the data product entry that contains the list of materialized views
                    # for the data product
                    db_dict[data_product['id']]['materializedViews'] = {}

                    # For each materialized view, add the name, description and SQL used to create the materialized view
                    for mview in dp_response['materializedViews']:
                        db_dict[data_product['id']]['materializedViews'][mview['name']] = {}
                        if mview.__contains__('definitionQuery'):
                            db_dict[data_product['id']]['materializedViews'][mview['name']]['sql'] = mview[
                                'definitionQuery']
                        if mview.__contains__('description'):
                            db_dict[data_product['id']]['materializedViews'][mview['name']]['description'] = mview[
                                'description']
                        if mview.__contains__('definitionProperties'):
                            if mview['definitionProperties'].__contains__('refresh_interval'):
                                db_dict[data_product['id']]['materializedViews'][mview['name']]['refreshInterval'] = \
                                mview['definitionProperties']['refresh_interval']  # TODO
                            if mview['definitionProperties'].__contains__('incremental_column'):
                                db_dict[data_product['id']]['materializedViews'][mview['name']]['incrementalColumn'] = \
                                mview['definitionProperties']['incremental_column']  # TODO

                        # Add a nested dictionary for the materialized view in the data product entry that contains the list
                        # of columns for that materialized view
                        db_dict[data_product['id']]['materializedViews'][mview['name']]['columns'] = {}

                        # For each column in the materialized view, add the name and description
                        for column in mview['columns']:
                            if column.__contains__('description'):
                                db_dict[data_product['id']]['materializedViews'][mview['name']]['columns'][
                                    column['name']] = column['description']  # TODO

                # Call to the function to get all tags for the data product
                dp_tags_response = self.get_data_product_tags(data_product_id=data_product['id'])
                print(dp_tags_response)
                tags = list()

                if dp_tags_response:
                    # Add each tag to the dictionary entry for the data product
                    for dp_tag in dp_tags_response:
                        tags.append(dp_tag['value'].replace(' ', '_'))  # TODO tags for Phase 4
                db_dict[data_product['id']]['tags'] = tags

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

        return db_dict

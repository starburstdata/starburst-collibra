## Common Packages

import os
import json
import six
import time
import markdown

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import logging

from pathlib import Path

# Importing Collibra Packages
from collibra_core.api_client import Configuration as Collibra_Core_Api_Client_Config
from collibra_core.api_client import ApiClient as Collibra_Core_Api_Client
from collibra_core.api.jobs_api import JobsApi
from collibra_core.api.assets_api import AssetsApi
from collibra_core.api.statuses_api import StatusesApi
from collibra_core.api.asset_types_api import AssetTypesApi
from collibra_core.api.attribute_types_api import AttributeTypesApi
from collibra_core.api.communities_api import CommunitiesApi
from collibra_core.api.domains_api import DomainsApi
from collibra_importer.api_client import Configuration as Collibra_Importer_Api_Client_Config
from collibra_importer.api_client import ApiClient as Collibra_Importer_Api_Client
from collibra_importer.api.import_api import ImportApi

from starburst import starburst_service

## Collibra Service Class
class CollibraService(object):

    import_batch_size = 1000

    def __init__(self, jobsApi = None, importApi = None, tmp_dir = '/tmp'):
        self.importApi = importApi
        self.jobsApi = jobsApi
        self.tmp_dir = tmp_dir
            
    def import_assets(self, import_request:str, batch_size = import_batch_size ):
        file_name = str(time.time()) + '.json'

        # Python requires request json written to disk before making an api call with attachment
        self.write_file(file_name, import_request)

        # Submit the JSON request then recieve a job response asynchronously
        import_job_response = self.importApi.import_json_in_job(file_name = 'import_assets.json', delete_file = True, file = self.tmp_dir + os.sep + file_name, batch_size = batch_size)
        
        job_state = import_job_response.state if import_job_response else 'ERROR'
        
        # Check the state of the import job
        while job_state != 'COMPLETED' and job_state != 'CANCELED' and job_state != 'ERROR':
            job_response = self.jobsApi.get_job(import_job_response.id)
            job_state = job_response.state
            if(job_state == 'ERROR'):
                logging.error(f"Message: {job_response.id} - {job_response.message}")
                return None
            time.sleep(1)
        
        # Delete File
        self.delete_file(self.tmp_dir + os.sep + file_name)

        return job_state

    # Delete a file from the disk
    def delete_file(self, path):
        os.remove(path)

    # Read a file from the disk
    def read_file(self, path):
        f = open(path, "r")
        return json.load(f)

    # Write a file to the disk
    def write_file(self, file_name:str, json_text:str):
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        p = Path(self.tmp_dir)
        p.mkdir(exist_ok=True)
        (p / file_name).open('w').write(json.dumps(json_text, indent=4, sort_keys=True))

# ImportCommand class for the Import API to easily construct the JSON request
class ImportCommand:

    swagger_types = {
        'resource_type': 'str',
        'identifier': 'dict',
        'display_name': 'str',
        'parent': 'dict',
        'type': 'dict',
        'attributes': 'dict',
        'relations': 'dict',
        'tags': 'list[str]',

    }

    attribute_map = {
        'resource_type': 'resourceType',
        'identifier': 'identifier',
        'display_name': 'displayName',
        'parent': 'parent',
        'type': 'type',
        'attributes': 'attributes',
        'relations': 'relations',
        'tags': 'tags'
    }

    def __init__(self, resource_type = None, identifier = None, display_name = None, parent = None, type = None, attributes = None, relations = None, tags = None):
        self._resource_type = None
        self._identifier = None
        self._display_name = None
        self._parent = None
        self._type = None
        self._attributes = None
        self._relations = None
        self._tags = None
        self.resource_type = resource_type
        self.identifier = identifier
        self.display_name = display_name
        self.parent = parent
        self.type = type
        self.attributes = attributes
        self.relations = relations
        self.tags = tags

    @property
    def resource_type(self):
        return self._resource_type

    @resource_type.setter
    def resource_type(self, resource_type):
        self._resource_type = resource_type

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, identifier):
        self._identifier = identifier

    @property
    def display_name(self):
        return self._display_name

    @display_name.setter
    def display_name(self, display_name):
        self._display_name = display_name

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        self._parent = parent

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type):
        self._type = type

    @property
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, attributes):
        self._attributes = attributes

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, relations):
        self._relations = relations
    
    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags = tags

    def build(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if value:
                if isinstance(value, list):
                    result[self.attribute_map[attr]] = list(map(
                        lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                        value
                    ))
                elif hasattr(value, "to_dict"):
                    result[self.attribute_map[attr]] = value.to_dict()
                elif isinstance(value, dict):
                    result[self.attribute_map[attr]] = dict(map(
                        lambda item: (item[0], item[1].to_dict())
                        if hasattr(item[1], "to_dict") else item,
                        value.items()
                    ))
                else:
                    result[self.attribute_map[attr]] = value
        if issubclass(ImportCommand, dict):
            for key, value in self.items():
                result[self.attribute_map[key]] = value

        return result


class StarburstCollibraFacade(object):

    def __init__(self, sep_url = None, sep_user = None, sep_role = None, sep_pwd = None, collibra_url = None, collibra_user = None, collibra_pwd = None, tmp_dir = None):
        self.sep_url = sep_url
        self.sep_user = sep_user
        if sep_role:
            self.sep_role = 'system=ROLE{' + sep_role + '}'
        else:
            self.sep_role = sep_role
        self.sep_pwd = sep_pwd
        self.collibra_url = collibra_url + '/rest/2.0'
        self.collibra_user = collibra_user
        self.collibra_pwd = collibra_pwd
        self.tmp_dir = tmp_dir
        
        collibra_core_config = Collibra_Core_Api_Client_Config()
        collibra_core_config.host = collibra_url + '/rest/2.0'
        collibra_core_config.username = collibra_user
        collibra_core_config.password = collibra_pwd
        collibra_core_api_client = Collibra_Core_Api_Client(collibra_core_config)
        self.assetsApi = AssetsApi(collibra_core_api_client)
        self.jobsApi = JobsApi(collibra_core_api_client)
        self.communitiesApi = CommunitiesApi(collibra_core_api_client)
        self.domainsApi = DomainsApi(collibra_core_api_client)

        collibra_importer_config = Collibra_Importer_Api_Client_Config()
        collibra_importer_config.host = collibra_url + '/rest/2.0'
        collibra_importer_config.username = collibra_user
        collibra_importer_config.password = collibra_pwd
        collibra_importer_api_client = Collibra_Importer_Api_Client(collibra_importer_config)
        self.importApi = ImportApi(collibra_importer_api_client)


    ## Import All Data Domains from Starburst to Collibra
    def query_and_import_data_domains(self, community = None):

        if not community:
            print ('ERROR:  Unable to import data domains due to missing "community" parameter.')
            return None
        
        print ('INFO:  Pulling all data domains from Starburst')
        get_data_domains_response = starburst_service.StarburstService(sep_url = self.sep_url, sep_user = self.sep_user, sep_role = self.sep_role, sep_pwd = self.sep_pwd).get_data_domains()
        if get_data_domains_response:
            # Build Request
            data_domains_import_request = list()
            # Create Domain Data Domains Import Command
            domain_data_domains_import_command = ImportCommand()
            domain_data_domains_import_command.resource_type = 'Domain'
            domain_data_domains_import_command.identifier = {
                        'name': 'Business Domains',
                        'community': {
                            'name': community
                        }
                    }
            domain_data_domains_import_command.type = {
                            'name': 'Business Dimensions'
                        }

            data_domains_import_request.append(domain_data_domains_import_command.build())

            for get_data_domain in get_data_domains_response:
                domain_import_command = ImportCommand()
                domain_import_command.resource_type = 'Asset'
                domain_import_command.identifier = {
                        'name': get_data_domain['name'],
                        'domain': {
                            'name': 'Business Domains',
                            'community': {
                                'name': community
                            }
                        }
                    }
                domain_import_command.type = {
                        'name': 'Starburst Business Domain'
                    }
                domain_import_command.attributes = {
                                    'Data Source': [{
                                        'value': 'Starburst'
                                    }]
                                }    
                if get_data_domain.__contains__('description'):
                    domain_import_command.attributes['Description'] = [{
                                        'value': get_data_domain['description']
                                    }]
                if get_data_domain.__contains__('schemaLocation'):
                    domain_import_command.attributes['Location'] = [{
                                        'value': get_data_domain['schemaLocation']
                                    }]
                data_domains_import_request.append(domain_import_command.build())
            
            if data_domains_import_request:
                print ('INFO:  Importing all data domains to Collibra')
                CollibraService(jobsApi = self.jobsApi, importApi = self.importApi, tmp_dir = self.tmp_dir).import_assets(import_request = data_domains_import_request)
            
        return get_data_domains_response

    ## Update All Data Product Views from Starburst to Collibra
    def update_data_product_views(self, product_views = None):
        if product_views:
            print ('INFO:  About to update Data Product Views')
            data_product_views_import_request = []
            for product_view in product_views:
                view_import_command = ImportCommand()
                view_import_command.resource_type = 'Asset'
                view_import_command.identifier = product_view['identifier']
                view_import_command.attributes = product_view['attributes']
                
                data_product_views_import_request.append(view_import_command.build())

            if data_product_views_import_request:
                CollibraService(jobsApi = self.jobsApi, importApi = self.importApi, tmp_dir = self.tmp_dir).import_assets(import_request = data_product_views_import_request)

            # Update View Columns
            self.update_data_product_view_columns(product_views = product_views)

    ## Import All Data Products from Starburst to Collibra
    def query_and_import_data_products(self, community = None, system_id = None):

        if not community and not system_id:
            print ('ERROR:  Unable to import data products due to missing "community" and/or "system ID" parameters.')
            return None
        
        print ('INFO:  Pulling all data products from Starburst')
        get_data_products_response = starburst_service.StarburstService(sep_url = self.sep_url, sep_user = self.sep_user, sep_role = self.sep_role, sep_pwd = self.sep_pwd).get_data_products()
        if get_data_products_response:
            data_products_import_request = []
            product_views = list() # All product views to be added to the view import command
            for product_id, get_data_product in get_data_products_response.items():
                found_domain_data_products_import_com = list()
                for domain_data_products_import_com in data_products_import_request:
                    domain_data_products_import_com = ImportCommand(domain_data_products_import_com)
                    if domain_data_products_import_com.resource_type=='Domain' and domain_data_products_import_com.identifier.name == f'{get_data_product["domainName"]} Data Products' and domain_data_products_import_com.type == 'Data Product Catalog':
                        found_domain_data_products_import_com.append(domain_data_products_import_com)

                # Create Domain Data Products Import Command if not found
                if not found_domain_data_products_import_com:
                    domain_data_products_import_command = ImportCommand()
                    domain_data_products_import_command.resource_type = 'Domain'
                    domain_data_products_import_command.identifier = {
                                'name': f'{get_data_product["domainName"]} Data Products',
                                'community': {
                                    'name': community
                                }
                            }
                    domain_data_products_import_command.type = {
                                    'name': 'Data Product Catalog'
                                }
                    data_products_import_request.append(domain_data_products_import_command.build())

                product_import_command = ImportCommand()
                product_import_command.resource_type = 'Asset'
                product_import_command.identifier = {
                        'name': get_data_product['name'],
                        'domain': {
                            'name': f'{get_data_product["domainName"]} Data Products',
                            'community': {
                                'name': community
                            }
                        }
                    }
                product_import_command.type = {
                        'name': 'Starburst Data Product'
                    }
                
                product_import_command.attributes = {
                                    'Data Source': [{
                                        'value': 'Starburst'
                                    }],
                                    'Catalog Name': [{
                                        'value': get_data_product['catalogName']
                                    }],
                                    'Schema Name': [{
                                        'value': get_data_product['schemaName']
                                    }],
                                    'Definition': [{
                                        'value': get_data_product['summary']
                                    }],
                                    'Description': [{
                                        'value': get_data_product['description'].replace("\n", "<br>" )
                                    }]
                                }
                
                if get_data_product.__contains__('productOwners'):
                    product_owners = list()
                    for product_owner_name, product_owner_email in get_data_product['productOwners'].items():
                        product_owners.append({ 'value': f'{product_owner_name} ({product_owner_email})'})

                    if product_owners:
                        product_import_command.attributes['Product Owner'] = product_owners

                if get_data_product.__contains__('tags') and len(get_data_product['tags'])>0:
                    product_import_command.tags = get_data_product['tags']

                # [Starburst Data Product] is classified by [Starburst Business Dimention]
                product_import_command.relations = {
                                '619c2d32-feb3-4913-92cd-144d8814fedf:SOURCE': [
                                    {
                                        'name': get_data_product['domainName'],
                                        'domain': {
                                            'name': 'Business Domains',
                                            'community': {
                                                'name': community
                                            }
                                        }
                                    }
                                ]
                            }
                
                related_product_views_identifiers = list()
                # TODO Query for the Community and System Name using the system id
                system_asset = self.assetsApi.get_asset(asset_id=system_id)

                if system_asset:
                    
                    domain_of_system_asset = self.domainsApi.get_domain(domain_id=system_asset.domain.id)
                    #community_of_system_asset = communitiesApi.get_community(community_id=domain_of_system_asset['community']['id'])

                    # Build the name of the Domain 'System > Catalog > Schema'
                    catalog_domain_name = f'{system_asset.name} > {get_data_product["catalogName"]} > {get_data_product["schemaName"]}'
                    catalog_domains = self.domainsApi.find_domains(community_id=domain_of_system_asset.community.id,name=catalog_domain_name,name_match_mode='EXACT')
                    
                    if not catalog_domains or not catalog_domains.results:
                        print (f'WARN:  Missing catalog schema {catalog_domain_name} in Collibra')

                    if catalog_domains:
                        for catalog_domain_asset in catalog_domains.results:

                            table_asset_type_id = '00000000-0000-0000-0000-000000031007'
                            database_view_asset_type_id = '00000000-0000-0000-0001-000400000009'
                            
                            if get_data_product.__contains__('views'):
                                # Build the name of the Domain 'System>Catalog>Schema>View'
                                view_assets=self.assetsApi.find_assets(domain_id=catalog_domain_asset.id,name=f'{system_asset.name}>{get_data_product["catalogName"]}>{get_data_product["schemaName"]}>',name_match_mode='START',type_ids=[database_view_asset_type_id])
                            
                                if not view_assets or not view_assets.results:
                                    print (f'INFO:  No views found in the "{get_data_product["name"]}" data product.')

                                for view_name, view_info in get_data_product['views'].items():
                                    found_view_asset = [view_asset for view_asset in view_assets.results if f'{system_asset.name}>{get_data_product["catalogName"]}>{get_data_product["schemaName"]}>{view_name}' == view_asset.name]
                                    if found_view_asset:
                                        view_to_update = {
                                                'domain_id': catalog_domain_asset.id,
                                                'name': found_view_asset[0].name,
                                                'identifier': {
                                                    'id': found_view_asset[0].id
                                                },
                                                'attributes':{
                                                        'Definition Query': [{
                                                            'value': view_info['sql']
                                                        }]
                                                    },
                                                'columns': view_info['columns']
                                            }

                                        if 'description' in view_info:
                                            view_to_update['attributes']['Description from source system'] = [{
                                                            'value': view_info['description']
                                                        }]
                                        related_product_views_identifiers.append(view_to_update['identifier'])
                                        product_views.append(view_to_update)

                            if get_data_product.__contains__('materializedViews'):
                                # Build the name of the Domain 'System>Catalog>Schema>View'
                                view_assets=self.assetsApi.find_assets(domain_id=catalog_domain_asset.id,name=f'{system_asset.name}>{get_data_product["catalogName"]}>{get_data_product["schemaName"]}>',name_match_mode='START',type_ids=[table_asset_type_id])
                            
                                if not view_assets or not view_assets.results:
                                    print (f'INFO:  No materialized views found in the "{get_data_product["name"]}" data product.')

                                for view_name, view_info in get_data_product['materializedViews'].items():
                                    found_view_asset = [view_asset for view_asset in view_assets.results if f'{system_asset.name}>{get_data_product["catalogName"]}>{get_data_product["schemaName"]}>{view_name}' == view_asset.name]
                                    if found_view_asset:
                                        view_to_update = {
                                                'domain_id': catalog_domain_asset.id,
                                                'name': found_view_asset[0].name,
                                                'identifier': {
                                                    'id': found_view_asset[0].id
                                                },
                                                'attributes':{
                                                        'Definition Query': [{
                                                            'value': view_info['sql']
                                                        }],
                                                        'Is Materialized View': [{
                                                            'value': "True"
                                                        }],
                                                        'Refresh Frequency': [{ # Add this to Attributes: definitionProperties
                                                            'value': view_info['refreshInterval']
                                                        }]
                                                    },
                                                'columns': view_info['columns']
                                            }
                                        
                                        if 'description' in view_info:
                                            view_to_update['attributes']['Description from source system'] = [{
                                                            'value': view_info['description']
                                                        }]
                                            
                                        if 'incrementalColumn' in view_info:
                                            view_to_update['attributes']['Incremental Column'] = [{
                                                            'value': view_info['incrementalColumn']
                                                        }]

                                        related_product_views_identifiers.append(view_to_update['identifier'])
                                        product_views.append(view_to_update)

                            # Reused [Data Structure] source for [Report] relation type - 69cec74b-9db0-4d79-a224-37280dd616e2
                            # [Data Product] asset type is a child of [Report]
                            # [Database View] asset type is a child of [Data Structure]
                            # [Table] asset type is a child of [Data Structure]
                            product_import_command.relations['69cec74b-9db0-4d79-a224-37280dd616e2:SOURCE'] = related_product_views_identifiers

                data_products_import_request.append(product_import_command.build())

            if data_products_import_request:
                CollibraService(jobsApi = self.jobsApi, importApi = self.importApi, tmp_dir = self.tmp_dir).import_assets(import_request = data_products_import_request)
        
            # Update Product View's details if there is any
            if product_views:
                self.update_data_product_views(product_views = product_views)

        return get_data_products_response

    ## Update All Data Product View Columns from Starburst to Collibra
    def update_data_product_view_columns(self, product_views = None):
        # Make sure it exist first before updating the Description
        column_asset_type_id = '00000000-0000-0000-0000-000000031008'
        data_product_view_columns_import_request = []
        for product_view in product_views:
            if product_view.__contains__('columns'):
                view_column_assets=self.assetsApi.find_assets(domain_id=product_view['domain_id'],name=f'{product_view["name"]}>',name_match_mode='START',type_ids=[column_asset_type_id])
            
                if not view_column_assets or not view_column_assets.results:
                    print (f'WARN:  No columns found in Collibra for view/materialized view\n\t--> {product_view["name"]}')
                    continue

                for view_column, view_column_description in product_view['columns'].items():
                    #found_view_column_assets = [view_column_asset for view_column_asset in view_column_assets.results if view_column_asset.name.endswith(f'>{view_column.upper()}')]
                    found_view_column_assets = [view_column_asset for view_column_asset in view_column_assets.results if view_column_asset.name.endswith(f'>{view_column}(column)')]
                
                    if found_view_column_assets:
                        view_column_import_command = ImportCommand()
                        view_column_import_command.resource_type = 'Asset'
                        view_column_import_command.identifier = {
                                'id': found_view_column_assets[0].id
                            }
                        
                        if view_column_description:
                            view_column_import_command.attributes = {
                                                'Description from source system': [{
                                                    'value': view_column_description
                                                }]
                                            }
                        
                        data_product_view_columns_import_request.append(view_column_import_command.build())

        if data_product_view_columns_import_request:
            CollibraService(jobsApi = self.jobsApi, importApi = self.importApi, tmp_dir = self.tmp_dir).import_assets(import_request = data_product_view_columns_import_request)

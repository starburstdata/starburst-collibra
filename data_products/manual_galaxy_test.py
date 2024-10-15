from starburst import galaxy_service
import boto3
import os
import json
import six

from botocore.exceptions import ClientError


account_domain = 'sbdemoposulliv.galaxy.starburst.io'
boto3.setup_default_session(profile_name='starburstdata-customer-success')
session = boto3.session.Session()
client = boto3.client('secretsmanager', os.environ['AWS_REGION'])
try:
    get_secret_value_response = client.get_secret_value(
        SecretId='posulliv/galaxy_api_creds'
    )
except ClientError as e:
    # For a list of exceptions thrown, see
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    raise e

galaxy_api_creds = json.loads(get_secret_value_response['SecretString'])
galaxy_api_creds['client_id'] = galaxy_api_creds['client_id'].replace('\"', '')
galaxy_api_creds['client_secret'] = galaxy_api_creds['client_secret'].replace('\"', '')
galaxy_service = galaxy_service.StarburstGalaxyService(
    account_domain=account_domain,
    client_id=galaxy_api_creds['client_id'],
    client_secret=galaxy_api_creds['client_secret']
)

data_products = galaxy_service.get_data_products()
print('final answer')
print(data_products)


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

    def __init__(self, resource_type=None, identifier=None, display_name=None, parent=None, type=None, attributes=None,
                 relations=None, tags=None):
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


community = 'comm'
system_id = 'system'

data_products_import_request = []
product_views = list() # All product views to be added to the view import command
for product_id, get_data_product in data_products.items():
    print(product_id)
    found_domain_data_products_import_com = list()
    for domain_data_products_import_com in data_products_import_request:
        if domain_data_products_import_com.resource_type == 'Domain' and domain_data_products_import_com.identifier.name == f'Starburst Galaxy Data Products' and domain_data_products_import_com.type == 'Data Product Catalog':
            found_domain_data_products_import_com.append(domain_data_products_import_com)

    # Create Domain Data Products Import Command if not found
    if not found_domain_data_products_import_com:
        domain_data_products_import_command = ImportCommand()
        domain_data_products_import_command.resource_type = 'Domain'
        domain_data_products_import_command.identifier = {
            'name': f'Starburst Galaxy Data Products',
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
            'name': f'Starburst Galaxy Data Products',
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
            'value': get_data_product['description'].replace("\n", "<br>")
        }]
    }

    if get_data_product.__contains__('productOwners'):
        product_owners = list()
        for product_owner_name, product_owner_email in get_data_product['productOwners'].items():
            product_owners.append({'value': f'{product_owner_name} ({product_owner_email})'})

        if product_owners:
            product_import_command.attributes['Product Owner'] = product_owners

    if get_data_product.__contains__('tags') and len(get_data_product['tags']) > 0:
        product_import_command.tags = get_data_product['tags']

    data_products_import_request.append(product_import_command.build())

print(data_products_import_request)

import sys
import time
import requests
from requests.exceptions import HTTPError


host='localhost'
port=8080
sep_user='sep_service_user'


def create_domain(name):
    api_url = f'http://{host}:{port}/api/v1/dataProduct/domains/'
    try:
        # Issue API call to get all data domains from Starburst
        response = requests.post(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            },
            json={
                'name': name
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def list_domains():
    api_url = f'http://{host}:{port}/api/v1/dataProduct/domains/'
    try:
        # Issue API call to get all data domains from Starburst
        response = requests.get(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def create_data_product(name, catalog_name, summary, domain_id, views=None):
    if views is None:
        views = []
    api_url = f'http://{host}:{port}/api/v1/dataProduct/products/'
    try:
        response = requests.post(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            },
            json={
                'name': name,
                'catalogName': catalog_name,
                'dataDomainId': domain_id,
                'summary': summary,
                'owners': [
                    {
                        'name': sep_user,
                        'email': 'email@starburst.io'
                    }
                ],
                'views': views
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def list_data_products():
    api_url = f'http://{host}:{port}/api/v1/dataProduct/products/'
    try:
        response = requests.get(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def publish_data_product(data_product_id):
    api_url = f'http://{host}:{port}/api/v1/dataProduct/products/{data_product_id}/workflows/publish'
    try:
        response = requests.post(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def publish_data_product_status(data_product_id):
    api_url = f'http://{host}:{port}/api/v1/dataProduct/products/{data_product_id}/workflows/publish'
    try:
        response = requests.get(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def sync_publish_data_product(data_product_id):
    publish_data_product(data_product_id)
    publish_status = publish_data_product_status(data_product_id)
    while publish_status['status'] == 'DRAFT' or publish_status['status'] == 'IN_PROGRESS':
        time.sleep(10)
        publish_status = publish_data_product_status(data_product_id)


def main():
    create_domain('domain_1')
    domains = list_domains()
    tpch_views = [
            {
                'name': 'nation_data_set',
                'definitionQuery': 'select name as nation_name from tpch.tiny.nation'
            },
            {
                'name': 'region_data_set',
                'definitionQuery': 'select name as region_name from tpch.tiny.region'
            }
        ]
    create_data_product(
        'data_product_1',
        'hive',
        'summary',
        domains[0]['id'],
        tpch_views
    )
    create_data_product(
        'data_product_2',
        'hive',
        'summary',
        domains[0]['id'],
        tpch_views
    )
    data_products = list_data_products()
    for data_product in data_products:
        sync_publish_data_product(data_product['id'])


if __name__ == '__main__':
    sys.exit(main())
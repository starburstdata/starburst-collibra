from starburst import galaxy_service
import boto3
import os
import json

from botocore.exceptions import ClientError

class TestGalaxyDataProducts:

    def setup_class(self):
        self.account_domain = 'sbdemoposulliv.galaxy.starburst.io'
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
        self.galaxy_service = galaxy_service.StarburstGalaxyService(
            account_domain=self.account_domain,
            client_id=galaxy_api_creds['client_id'],
            client_secret=galaxy_api_creds['client_secret']
        )

    def test_data_products(self):
        data_products = self.galaxy_service.get_data_products()
        print(data_products)
        assert data_products is None

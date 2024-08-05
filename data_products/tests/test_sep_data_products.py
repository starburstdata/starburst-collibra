from starburst import starburst_service


class TestSepDataProducts:

    def setup_class(self):
        self.host = 'localhost'
        self.port = 8080
        self.sep_url = f'http://{self.host}:{self.port}'
        self.sep_user = 'sep_service_user'
        self.sep_service = starburst_service.StarburstService(
            sep_url=self.sep_url,
            sep_user=self.sep_user,
            sep_pwd=''
        )

    def test_domain_exists(self):
        domains = self.sep_service.get_data_domains()
        assert len(domains) == 1
        assert domains[0]['name'] == 'domain_1'

    def test_data_products(self):
        data_product_names = ['data_product_1', 'data_product_2']
        for data_product in data_product_names:
            self.check_data_product(data_product)

    def check_data_product(self, data_product_name):
        data_product = list(self.sep_service.get_data_products(data_product_name).values())[0]
        assert data_product is not None
        assert data_product['catalogName'] == 'hive'
        assert data_product['schemaName'] == data_product_name
        assert data_product['createdBy'] == self.sep_user

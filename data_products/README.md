# SUMMARY
This integration is designed to extract all [data domains](https://docs.starburst.io/latest/data-products/domain-management.html#create-a-domain) and all [published data products](https://docs.starburst.io/latest/data-products/index.html) from a Starburst Enterprise cluster and then catalog those assets in Collibra.  At a high-level, this integration performs the following tasks:
 * Create assets in Collibra for each published data product in Starburst
 * Create assets for each domain the data products are associated with
 * Extract the aforementioned data products metadata from Starburst
 * Add the metadata to the corresponding assets in Collibra
 * Link the data domain asset to the appropriate data product asset
 * Link the data product asset to the appropriate data sets

Prior to running the integration, the data sets associated with your Starburst data products will look similar to what is shown below.  The data sets will be cataloged as views or tables and will linked to the Starburst datasource and system.  After running the integration, the data products and data domains will be added to the catalog (along with all of their metadata) and the data sets will be linked to the approriate data domains and data products.

![integration-overview](https://github.com/starburstdata/starburst-collibra/blob/main/data_products/integration-before-after.png?raw=true)


More information on this integration, including instructions for installing and running the integration, can be found on the [Collibra Marketplace listing](https://marketplace.collibra.com/listings/starburst-jdbc-driver/) page or in the [starburst-collibra-dataproducts-user-guide.pdf](https://github.com/starburstdata/starburst-collibra/blob/main/data_products/starburst-collibra-dataproducts-user-guide.pdf) file in this folder.

---
# QUICK INSTRUCTIONS
Full instructions for installing and running this integration can be found in the collibra_starburst_dataproducts.pdf file within this repository.  An overview of the steps are provided below for quick reference.
1. Download all of the files in this folder to the location where you want to run the integration from.
2. Confirm the device you are running this integration from has access to your Collibra and Starburst Enterprise environments.
3. Confirm the device meets the system requirements.
    * Valid license for Collibra Integration Cloud, Collibra Catalog and Starburst Enterprise
    * Collibra Integration Cloud v2021+
    * Starburst Enterprise 380-e LTS+
    * Python 3.9+
    * Jupyter Notebook
5. Confirm you have created a folder in the location where you downloaded the integration files.  This folder will hold temporary files used by the integration (you can name it whatever you want).
6. Run the Jupyter notebook by starting at the first cell and continuing through to the last cell.
7. Confirm all of the expected asssets have been created in your Collibra instance by using the [starburst_collibra_integration.xlsx](https://github.com/starburstdata/starburst-collibra/blob/main/data_products/cma/starburst_collibra_integration.xlsx) as a guide.

___
# DEMO
The video below provides a brief walkthrough of running this integration.
![integration-video](https://github.com/starburstdata/starburst-collibra/blob/main/data_products/dbt_Lightning_Demo.mp4?raw=true)

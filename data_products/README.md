This integration is designed to extract all data domains and all published data products from a Starburst Enterprise cluster and then catalog those assets in Collibra.  At a high-level, this integration performs the following tasks:
 * Create assets in Collibra for each published data product in Starburst
 * Create assets for each domain the data products are associated with
 * Extract the aforementioned data products metadata from Starburst
 * Add the metadata to the corresponding assets in Collibra
 * Link the data domain asset to the appropriate data product asset
 * Link the data product asset to the appropriate data sets

Prior to running the integration, the data sets associated with your Starburst data products will look similar to what is shown below.  The data sets will be cataloged as views or tables and will linked to the Starburst datasource and system.

![integration-before](https://github.com/starburstdata/starburst-collibra/blob/main/data_products/collibra-dp-before.png?raw=true)

After running the integration, the data products and data domains will be added to the catalog (along with all of their metadata) and the data sets will be linked to the approriate data domains and data products.

![integration-after](https://github.com/starburstdata/starburst-collibra/blob/main/data_products/collibra-dp-after.png?raw=true)


More information on this integration, including instructions for installing and running the integration, can be found on the [Collibra Marketplace listing](https://marketplace.collibra.com/listings/starburst-jdbc-driver/) page.

# Azure RPDE Proxy
Azure RPDE Proxy allowing client self-registration

[![Deploy to Azure](https://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fopenactive%2Fazure-rpde-proxy%2Fmaster%2FAzureRpdeProxyDeployment%2Fazuredeploy.json) [![Visualise Deployment](http://armviz.io/visualizebutton.png)](http://armviz.io/#/?load=https%3A%2F%2Fraw.githubusercontent.com%2Fopenactive%2Fazure-rpde-proxy%2Fmaster%2FAzureRpdeProxyDeployment%2Fazuredeploy.json)

## Overview

The proxy makes use of a serverless architecture with controlled throughput to provide scalability with a cap on cost. One proxy can be used by all source RPDE servers, and accepts feed registrations from individual source RPDE server instances.

1. When the source RPDE server is initialised, it performs a simple call to register itself with the proxy, which includes the alias  (an alphanumeric name for the feed) configured in app.config. The source RPDE server dataset site will reflect the proxy feed URLs instead of the source RPDE server’s own feed URLs.

2. The feed list stores a list of all registered feeds. If a new entry is added to the feed list by the registration endpoint, the URL of the first page from that feed is added to the processing queue.

3. A Service Bus queue provides a queue to store the next page in the feed.

4. An Azure function is triggered by the queue to read the next page from each feed.

5. The page is split into items and stored in Azure SQL.

6. The OpenActive RPDE feed endpoint accepts the a feed alias as part of its path, and reads the next items from Azure SQL using the alias to constrain the query

7. The free Cloudflare CDN is used to minimise load on Azure SQL. 

## Resources

This proxy uses the following resources:

- Storage Account
- Function App
    - App Settings Configuration
    - Source Control Deployment
- Service Bus
	- 3 queues
- Azure SQL Server
	- Azure SQL Database
- App Service plan


## Deployment

The deployment template above deploys the Azure RPDE Proxy Function App and links it to continous integration, to allow updates to be deployed from this GitHub repository.

### Deploy Steps:

- Deploy this repository to Azure, using the button above

- Connect to the database using the credentials in the Deployment output within the Azure portal, and run database.sql.

- The AzureProxyRegistrationKey can be found in the configuration of the Registration function, as the default function key.

- The status endpoint can be accessed using the default function key of the Status function.

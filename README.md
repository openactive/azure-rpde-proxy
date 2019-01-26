# Azure RPDE Proxy
Azure RPDE Proxy allowing client self-registration

[![Deploy to Azure](https://azuredeploy.net/deploybutton.png)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fopenactive%2Fazure-rpde-proxy%2Fmaster%2FAzureRpdeProxyDeployment%2Fazuredeploy.json) [![Visualise Deployment](http://armviz.io/visualizebutton.png)](http://armviz.io/#/?load=https%3A%2F%2Fraw.githubusercontent.com%2Fopenactive%2Fazure-rpde-proxy%2Fmaster%2FAzureRpdeProxyDeployment%2Fazuredeploy.json)

## Overview

The deployment template above deploys the Azure RPDE Proxy Function App and links it to continous integration, to allow updates to be deployed from this GitHub repository.

This following resources:

- Storage Account
- Function App
    - App Settings Configuration
    - Source Control Deployment
- Service Bus
	- 3 queues
- Azure SQL Server
	- Azure SQL Database
- App Service plan

## Deploy Steps:

- Deploy this repository to Azure, using the button above

- Connect to the database using the credentials in the Deployment output within the Azure portal, and run database.sql.

- The AzureProxyRegistrationKey can be found in the configuration of the Registration function, as the default function key.

- The status endpoint can be accessed using the default function key of the Status function.

# Provision a function app with source deployed from GitHub

<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fazure%2Fazure-quickstart-templates%2Fmaster%2F201-function-app-dedicated-github-deploy%2Fazuredeploy.json" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>
<a href="http://armviz.io/#/?load=https%3A%2F%2Fraw.githubusercontent.com%2FAzure%2Fazure-quickstart-templates%2Fmaster%2F201-function-app-dedicated-github-deploy%2Fazuredeploy.json" target="_blank">
    <img src="http://armviz.io/visualizebutton.png"/>
</a>

<a href="https://azuredeploy.net/" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>

## Overview

This template deploys the Azure RPDE Proxy Function App and links it to continous integration, to allow updates to be deployed from this GitHub repository.

This template deploys the following resources:

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

- Deploy this repository to Azure: 
<a href="https://azuredeploy.net/" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>

- Connect to the database using the credentials in the Deployment output within the Azure portal, and run database.sql.

- The AzureProxyRegistrationKey can be found in the configuration of the Registration function, as the default function key.

- The status endpoint can be accessed using the default function key of the Status function.

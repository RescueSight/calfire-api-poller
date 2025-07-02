# CalFire API Poller

CalFire has an API which will return a JSON of recent events and reports. To display this information on our frontend and be able to facilitate email alerts for inactive users, we need an Azure function to monitor this data, parse it, and upload it to CosmosDB.

This will act as a middleman between CalFire and RescueSight.

The project structure is very simple, requiring just one js file to execute. In this process, we have the ability to add custom metadata. This could be helpful for CosmosDB filtering down the line.

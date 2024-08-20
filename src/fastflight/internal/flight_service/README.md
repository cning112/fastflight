# Flight Data Server And Client


## How does it work?
Assuming the flight server is running, a user can create a client helper and use it to get data from various data sources.
See the example in `client.client_helpers.py`


## How to add a new data source type?
1. Add a new data source type to the enum in `models/data_source`
2. Add a new params class in `models/params`. Make sure the new params class is registered with the new data source type
3. Add a new data service to handle the new params
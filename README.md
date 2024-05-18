# BoardsOnFire Python Client

## Overview

The `BoardsOnFireClient` is a Python package that provides a convenient interface to interact with the BoardsOnFire API. It allows developers to easily manage organizations, users, entities, and data sources within the BoardsOnFire ecosystem.

## Installation

You can install the package using pip:

```bash
pip install boardsonfire-client
```

## Usage

### Initialization

To use the client, you need to initialize it with your domain and API key:

```python
from boardsonfire_client import BoardsOnFireClient

client = BoardsOnFireClient(domain='your-domain', api_key='your-api-key')
```

### Working with Organizations

You can list, retrieve, and manage organizations using the `Organizations` class.

#### List Organizations

```python
# List organizations with pagination
organizations = client.organizations.list(page_size=50, page=1)

# List all organizations
for org in client.organizations.list_all(limit=100):
    print(org)
```

#### Get an Organization by ID

```python
organization = client.organizations.get(id='organization-id')
print(organization)
```

### Working with Users

The `Users` class allows you to manage users within your BoardsOnFire instance.

#### List Users

```python
# List users with pagination
users = client.users.list(page_size=100, page=1)

# List all users
for user in client.users.list_all(limit=100):
    print(user)
```

#### Get a User by ID

```python
user = client.users.get(id='user-id')
print(user)
```

### Working with Entities

The `Entities` class provides methods to manage entity objects.

#### List Entities

```python
# List entity objects with pagination
entities = client.entities.list(entity_name='entity-name', page_size=100, page=1)

# List all entity objects
for entity in client.entities.list_all(entity_name='entity-name', limit=100):
    print(entity)
```

#### Get an Entity by ID

```python
entity = client.entities.get(entity_name='entity-name', id='entity-id')
print(entity)
```

#### Create an Entity

```python
new_entity = {
    "organization_id": "organization-id",
    # other fields
}
created_entity = client.entities.create(entity_name='entity-name', data=new_entity)
print(created_entity)
```

#### Update an Entity

```python
updated_entity_data = {
    "id": "entity_id",
    "field_to_update": "new-value",
    # other fields
}
updated_entity = client.entities.update(entity_name='entity-name', id='entity-id', data=updated_entity_data)
print(updated_entity)
```

#### Delete an Entity

```python
client.entities.delete(entity_name='entity-name', id='entity-id')
```

### Working with Data Sources

The `DataSources` class allows you to manage data objects within data sources.

#### List Data Objects

```python
# List data objects with pagination
data_objects = client.datasources.list(datasource_name='datasource-name', page_size=100, page=1)

# List all data objects
for data in client.datasources.list_all(datasource_name='datasource-name', limit=100):
    print(data)
```

#### Get a Data Object by ID

```python
data_object = client.datasources.get(datasource_name='datasource-name', id='data-id')
print(data_object)
```

#### Create a Data Object

```python
new_data = {
    "organization_id": "organization-id",
    "timestamp": "2023-01-01T00:00:00Z",
    # other fields
}
created_data = client.datasources.create(datasource_name='datasource-name', data=new_data)
print(created_data)
```

#### Update a Data Object

```python
updated_data = {
    "field_to_update": "new-value",
    # other fields
}
updated_data_object = client.datasources.update(datasource_name='datasource-name', id='data-id', data=updated_data)
print(updated_data_object)
```

#### Delete a Data Object

```python
client.datasources.delete(datasource_name='datasource-name', id='data-id')
```

## Error Handling

The client raises custom exceptions to handle various error scenarios:

- `RateLimitException`: Raised when the rate limit is exceeded.
- `NotFoundException`: Raised when the requested resource is not found.
- `RestClientException`: Raised for other unsuccessful responses or invalid JSON in the response.

Example:

```python
try:
    users = client.users.list(page_size=100, page=1)
except RateLimitException as e:
    print("Rate limit exceeded:", e)
except RestClientException as e:
    print("An error occurred:", e)

try:
    users = client.users.get(id='user_id')
except NotFoundException as e:
    print("User not found:", e)
```

## Logging

You can pass a custom logger to the client for logging purposes:

```python
import logging

logger = logging.getLogger('boardsonfire_client')
client = BoardsOnFireClient(domain='your-domain', api_key='your-api-key', logger=logger)
```

## Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss any changes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
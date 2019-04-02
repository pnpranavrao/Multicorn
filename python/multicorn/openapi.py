from multicorn import ForeignDataWrapper
from bravado.client import SwaggerClient # this should be pip installed

"""
This class lets you translate any OpenAPI compatible API into a Postgres Foreign Table (with caveats).

Dependencies
------------
``bravado`` needs to be installed. It is a Swagger/OpenAPI client.

Options
----------------
``resource_name`` (required)
  The REST Resource to be modelled as a table in Postgres
``swagger_definition``
  The OpenAPI specification of the endpoint in JSON 

Usage example
-------------
Suppose you want to read Github's API for all public repositories:

.. code-block:: sql

create schema github; #create a schema for each Endpoint.

#Create server to answer all queries to this endpoint
CREATE SERVER github_server foreign data wrapper multicorn options (
    wrapper 'multicorn.openapi.OpenApiWrapper',
    resource_name 'repositories',
    swagger_definition 'https://api.apis.guru/v2/specs/github.com/v3/swagger.json'
);

#Mention the columns to be fetched. This can be automated with the Swagger client through introspection.
CREATE FOREIGN TABLE github.repositories_ft (
    id character varying,
    url character varying,
    name character varying
) server github_server;

# Foreign tables are not visible through AWS Glue's connector. So expose as a View.
CREATE VIEW github.repositories as (
	select * from github.repositories_ft 
)

.. code-block:: bash

postgres=# select * from github.repositories;
 id  |                                url                                 |             name
-----+--------------------------------------------------------------------+------------------------------
 1   | https://api.github.com/repos/mojombo/grit                          | grit
 26  | https://api.github.com/repos/wycats/merb-core                      | merb-core
 27  | https://api.github.com/repos/rubinius/rubinius                     | rubinius
 28  | https://api.github.com/repos/mojombo/god                           | god
 29  | https://api.github.com/repos/vanpelt/jsawesome                     | jsawesome
 31  | https://api.github.com/repos/wycats/jspec                          | jspec

"""

class OpenApiWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(OpenApiWrapper, self).__init__(options, columns)
        print(columns)
        print("\n")
        print(options)
        self.columns =  columns #options["columns"] #"id,url,name".split(",")
        self.client = SwaggerClient.from_url(options["swagger_definition"], config = {'validate_responses': False, 'use_models': False})
        self.resource_name = options["resource_name"]


    def execute(self, quals, columns):
        resource = getattr(self.client, self.resource_name)
        results = getattr(resource,"get_"+self.resource_name)()
        for item in results.result():
            line = {}
            for column_name in self.columns:
                line[column_name] = item[column_name]
            yield line

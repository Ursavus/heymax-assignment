# Heymax take-home


The overall DB schema can be viewed in the following link:
https://dbdiagram.io/d/heymax_assignment-68441f065a9a94714e532997

The link to the engagement dashboard can be found at:
https://lookerstudio.google.com/reporting/fa5ba41b-34a3-4ca4-95f1-8c11c8a7dc49/page/pVPNF


To reduce the complexity of the project, a single Supabase DB was used. To simulate transferring and holding data between multiple DBs, different schemas in the DB were used. There are 3 schemas that hold all the information:
 - public: This acts as the source DB, and holds the raw data
 - orchestrator: This is the DB that serves the ingestion pipeline and holds data related to pipeline runs and logs
 - warehouse: This serves as a representation of a Data warehouse, where the data is gathered and aggregated
 - reporting: The reporting schema would typically be in the warehouse, it's preaggregated metrics that are used to power the visualisation in the dashboard

## ELT Pipeline and the Orchestrator DB
![Image](https://github.com/user-attachments/assets/5530f89a-4c2d-4b67-bd46-27920d48ca5a)

The Orchestrator DB holds the tables that inform the ELT pipeline and hold any logs necessary for debugging. A batch ELT was chosen for this project as it would create a read replica of the source data in the warehouse, ensuring that no information is lost and can be worked with, if the needs arises.

The Orchestrator allows for new sources of data to be added with minimal effort (assuming that the sources are SQL DBs and the tables have timestamp columns that can be used for batching), by adding new rows to the pipelines-definitions table.

The runs table hold all attempted runs, as well as some simple run stats like the elapsed-time and status to enable building an alert system if the need arises.

The logs table uses a log function in the IngestionPipeline class to log any data that needs to be captured, allowing for debugging.

The IngestionPipeline and the DB connection class are separated to separate the ingestion controller and the adapter. This should make it easier to make changes to either without greatly affecting the functionality of the other. The IngestionPipeline class accepts the pipeline_id as an input and uses the definition in the pipeline_definitions and the details from the previous run to either catch up (in controlled timeframes) or ingest any new data from the last run.

In this implementation, since there is no live data being added, a while loop is used to continuously trigger the ingestion pipeline, and the DBT transform. In a live environment a Cron job would be used to trigger the tasks.

## DBT transform and the Warehouse
![Image](https://github.com/user-attachments/assets/530cbfd0-7c5f-4eb8-8437-d339904bf1de)

Since Heymax is a platform that allows users to interact with businesses and businesses to have user traffic directed to them, the structure of the model captures this. The fact tables act as the methods through which individual users interact with the business. The fact tables are split by the type and depth of engagement. While this might increase the complexity of the schema, it makes it easier to query as each fact table holds a subset of the type of interactions.

The dimension tables in this case just hold simple bits of information, but they can be expanded if more information is available.

The other dimension that has been split out is the utm_source. While there were just a few unique values provided in this sample data, the purpose of this separation is to add more information regarding campaigns that were used to acquire customers, if that is required.

## Aggregation tables
![Image](https://github.com/user-attachments/assets/20450330-c9ac-44dd-adaf-bb9ddc472c17)
![Image](https://github.com/user-attachments/assets/575673b3-120c-4a07-9778-e3b5085cfdae)

While there is not much being done with these set of tables in this assignment, they act as intermediary between analysis and reporting usecases. The agg_ table can be used to quickly generate and track the different engagements of individual users and transaction categories. However, with the more data, this set of table allows users and transaction categories to be profiled, either with simple rule based methods, or a clustering algorithm based on their latest behaviour. Profiling users in this method allows for better understanding of the customer types and unlocks the ability to recommend new transaction categories to users.

## Reporting tables
![Image](https://github.com/user-attachments/assets/5a8349b6-5376-4b72-bf8b-ede4d7d75754)

This set of tables aggregates metrics that need to be tracked. These tables are then fed directly into the visualisation tool - Looker studio in this case - to allow dashboards to be generated with as little calculation required as possible.

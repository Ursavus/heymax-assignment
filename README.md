# heymax-assignment


The overall DB schema can be viewed in the following link:
https://dbdiagram.io/d/heymax_assignment-68441f065a9a94714e532997


To reduce the complexity of the project, a single Supabase DB was used. To simulate transferring and holding data between multiple DBs, different schemas in the DB were used. There are 3 schemas that hold all the information:
 - public: This acts as the source DB, and holds the raw data
 - orchestrator: This is the DB that serves the ingestion pipeline and holds data related to pipeline runs and logs
 - warehouse: This serves as a representation of a Data warehouse, where the data is gathered and aggregated


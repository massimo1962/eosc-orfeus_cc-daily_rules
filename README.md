## eosc-orfeus_cc-daily_rules
# ingestion and extract metadata from ORFEUS_CC daily files; apply some rules on data
Composed by WFmetadata extraction, DublinCore compute, and iRODS/B2safe ingestion & management.

The project give the capability to choose one sequence of action to be applied to the data files (mseed) and execute this job every time that we can specify e.g. into crontab.

Like  WFCatalogcollector script this project elaborate the actions (rules) and apply they to the digital object based on a list of files that fall into specific requirement (date, update, etc..) rely on very well done collector.
A separate file called ruleMp.json is in charge to implement the sequence and what steps are involved, a first look to the file will be better than a lot of words for describe it.
All the metadata extracted are inserted into mongoDB instance that is the same of the WFCollector (wfrepo) and the Dublin Core collection is integrated into this, iRODS integration is also performed in order to be able to expose our data via EUDAT/EOSC-HUB/EPOS ecosystem and to make our data more be FAIR.

Generation of PIDs and save info inside iRODS and MongoDB; make a EUDAT replication and many other activities are executed on regular base thanks to the iRODS rules and specific functions.

At this time we have a few rules and some actions but, in the future we can think about increase or change they following the ORFEUS_CC nodes needs. 


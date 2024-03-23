[<img src=https://user-images.githubusercontent.com/6883670/31999264-976dfb86-b98a-11e7-9432-0316345a72ea.png height=75 />](https://reactome.org)

# Reactome Interaction Importer

## What is the Reactome Interaction Importer project

The Interaction Importer is a tool used for importing IntAct Interaction data into the Reactome graph database.

#### Project components used:

* [Neo4j](https://neo4j.com/download/) Community Edition - version: 3.2.2 or latest
* Reactome [Graph](https://github.com/reactome/graph-core) Core

**Properties**

When executing the jar file following properties have to be set.
```java
Usage:
  org.reactome.server.graph.Main [--help] [(-h|--host) <host>] [(-s|--port)
  <port>] [(-d|--name) <name>] [(-u|--user) <user>] [(-p|--password) <password>]
  [(-f|--intactFile) <intactFile>] [(-b|--bar)[:<bar>]]

A tool for importing reactome data import to the neo4j graphDb


  [--help]
        Prints this help message.

  [(-h|--host) <host>]
        The database host (default: localhost)

  [(-s|--port) <port>]
        The reactome port (default: 3306)

  [(-d|--name) <name>]
        The reactome database name to connect to (default: reactome)

  [(-u|--user) <user>]
        The database user (default: reactome)

  [(-p|--password) <password>]
        The password to connect to the database (default: reactome)

  [(-f|--intactFile) <intactFile>]
        Path to the interaction data file
```

Example:
```bash
java -jar InteractionImporter-exec.jar \ 
     -h localhost \ 
     -s 3306 \
     -d reactome \ 
     -u reactome_user \
     -p not2share
```

#### Extras
* [1] [Reactome Graph Database](http://www.reactome.org/download/current/reactome.graphdb.tgz)
* [2] [Documentation](http://www.reactome.org/pages/documentation/developer-guide/graph-database/)
* [3] [MySQL dump database](http://www.reactome.org/download/current/databases/gk_current.sql.gz)

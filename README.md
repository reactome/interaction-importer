[<img src=https://user-images.githubusercontent.com/6883670/31999264-976dfb86-b98a-11e7-9432-0316345a72ea.png height=75 />](https://reactome.org)

# Reactome Interaction Importer

## What is the Reactome Interaction Importer project

The Interaction Importer is a tool used for importing IntAct Interaction data into the Reactome graph database.

#### Project components used:

* [Neo4j](https://neo4j.com/download/) Community Edition - version: 3.2.2 or latest
* Reactome [Graph](https://github.com/reactome/graph-core) Core

**Properties**

It connects to the graph over Bolt, after graph-importer has built and installed it, and adds
IntAct's interactions in one transaction: all of them, or on any failure none. A graph that
already has interactions is refused, so a rerun cannot add them twice.

```
Usage:
  org.reactome.server.graph.Main [--help] [(-h|--host) <host>] [(-s|--port) <port>]
  [(-d|--name) <name>] [(-u|--user) <user>] [(-p|--password) <password>]
  [(-f|--intactFile) <intactFile>] [(-q|--sqlLite)[:<sqlLite>]] [(-b|--bar)[:<bar>]]

  [(-h|--host) <host>]          The graph database's host (default: localhost)
  [(-s|--port) <port>]          Its Bolt port (default: 7687)
  [(-d|--name) <name>]          The database to write to (default: the server's default)
  [(-u|--user) <user>]          The graph database's user (default: neo4j)
  [(-p|--password) <password>]  Its password
  [(-f|--intactFile) <file>]    IntAct's intact-micluster.txt, or with -q an interactors SQLite
                                database; if not given, IntAct's current file is downloaded
  [(-m|--minimum) <n>]          The fewest interactions a run may add (default: 1); fewer and
                                nothing is committed. IntAct's parser reports a bad file only in
                                its log, so this is what stops a run on part of the file.
```

Example, as the release runs it (`Jenkinsfile`):
```bash
java -jar target/InteractionImporter-exec.jar -h localhost -d graph.db -m 70000 -u neo4j -p <password>
```

#### Checking it

`ci/check-fixture.sh` runs the importer against a small graph (`ci/fixture/graph.cypher`, with
the release graph's unique dbId and stId) and six rows of IntAct's own file, and checks it
writes what graph-importer did: which entities are targets, the ReferenceEntities it adds
(identifier, stId, species), the interactions and their interactors; that a second run is
refused; and that a run adding fewer than its minimum commits nothing. It needs Docker and a JDK 17:

```bash
mvn package -DskipTests && ci/check-fixture.sh
```

#### Extras
* [1] [Reactome Graph Database](http://www.reactome.org/download/current/reactome.graphdb.tgz)
* [2] [Documentation](http://www.reactome.org/pages/documentation/developer-guide/graph-database/)
* [3] [MySQL dump database](http://www.reactome.org/download/current/databases/gk_current.sql.gz)

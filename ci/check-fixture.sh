#!/usr/bin/env bash
# The importer against a fixture graph (ci/fixture/graph.cypher, with the release graph's
# unique dbId and stId) and six rows of IntAct's own file (ci/fixture/intact-micluster.txt,
# taken from intact-micluster.txt, its authors' names replaced): a partner already in the
# graph, a chain (-PRO_), partners of other species, an isoform matched by its
# variantIdentifier, a target through a catalyst, an entity no event uses, and one with no
# ReferenceDatabase, which is skipped. Checks what graph-importer wrote for each, that a
# second run is refused, and that a run adding fewer than its minimum commits nothing.
#
#   mvn package -DskipTests && ci/check-fixture.sh
#
# Needs Docker and a JDK 17 on PATH. Starts and removes its own Neo4j 4.3 container.
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=interaction-importer-fixture
PORT=${FIXTURE_PORT:-7699}
JAR=target/InteractionImporter-exec.jar
docker rm -f "$NAME" >/dev/null 2>&1 || true
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:7687" -e NEO4J_AUTH=none neo4j:4.3.24-community >/dev/null
for _ in $(seq 60); do docker exec "$NAME" cypher-shell "RETURN 1" >/dev/null 2>&1 && break; sleep 2; done
docker exec -i "$NAME" cypher-shell < ci/fixture/graph.cypher
q() { docker exec "$NAME" cypher-shell --format plain "$1" | tail -n +2; }
fail() { echo "FAIL: $1" >&2; exit 1; }
run() { java -jar "$JAR" -h localhost -s "$PORT" -u neo4j -p unused -f ci/fixture/intact-micluster.txt; }

run > /dev/null
[ "$(q 'MATCH (i:UndirectedInteraction) RETURN count(i)')" = 4 ] || fail "four interactions"
[ "$(q "MATCH (re:ReferenceIsoform {variantIdentifier: 'P05067-PRO_0000000092'}) RETURN re.identifier + ' ' + re.stId")" = '"P05067 uniprot:P05067#PRO_0000000092"' ] \
  || fail "a chain: its identifier and stId, as graph-importer gives them"
[ "$(q "MATCH (re:ReferenceGeneProduct {identifier: 'Q62796'})-[:species]->(s) RETURN re.stId + ' ' + s.taxId")" = '"uniprot:Q62796 10116"' ] \
  || fail "a partner of another species: its stId and its own species"
[ "$(q "MATCH (re:ReferenceEntity {variantIdentifier: 'O54918-2'}) RETURN count(re)")" = 1 ] \
  || fail "an isoform in the graph is matched by its variantIdentifier, not made again"
[ "$(q "MATCH (re:ReferenceEntity {identifier: 'P49418'}) RETURN count(re)")" = 1 ] || fail "a partner already in the graph is not made again"
[ "$(q "MATCH (:ReferenceEntity {identifier: 'Q8NFH8'})<-[:interactor]-(i) RETURN count(i)")" = 1 ] \
  || fail "a target through a catalyst alone, read by its identifier where its variantIdentifier is blank"
[ "$(q "MATCH (:ReferenceEntity {identifier: 'P12931'})<-[:interactor]-(i) RETURN count(i)")" = 0 ] \
  || fail "no interactions for an entity no event uses"
[ "$(q "MATCH (i:UndirectedInteraction)-[r:interactor]->(re) RETURN count(r)")" = 8 ] || fail "two interactors each"
[ "$(q "MATCH (i:UndirectedInteraction)-[:referenceDatabase]->(d:ReferenceDatabase {displayName: 'IntAct'}) RETURN count(i)")" = 4 ] \
  || fail "each interaction to IntAct"
[ "$(q "MATCH (e:InstanceEdit)-[:created]->(n) RETURN count(n)")" = 7 ] || fail "each node made is created by the importer"
if run > /dev/null 2>&1; then fail "a second run is refused"; fi
[ "$(q 'MATCH (i:UndirectedInteraction) RETURN count(i)')" = 4 ] || fail "and changes nothing"

# Fewer interactions than asked for (a file IntAct's parser stopped part way through, which
# it reports only in its log): nothing is committed, not even the importer's own nodes.
docker exec "$NAME" cypher-shell "MATCH (n) WHERE n.dbId > 2010 DETACH DELETE n" > /dev/null
before=$(q 'MATCH (n) RETURN count(n)')
if java -jar "$JAR" -h localhost -s "$PORT" -u neo4j -p unused -f ci/fixture/intact-micluster.txt -m 5 > /dev/null 2>&1; then
  fail "fewer interactions than the minimum fail the run"
fi
[ "$(q 'MATCH (n) RETURN count(n)')" = "$before" ] || fail "and commit nothing"
echo "ok: the fixture's interactions, as graph-importer wrote them"

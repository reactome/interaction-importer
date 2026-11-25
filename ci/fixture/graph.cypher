// A small graph as graph-importer builds one, for ci/check-fixture.sh: the release graph's
// unique dbId and stId (graph-importer's createConstraints), the two ReferenceDatabases the
// importer adds to by dbId, three species, and ReferenceEntities reached in each way that
// makes one a target, or not.
CREATE CONSTRAINT dbId_unique IF NOT EXISTS ON (n:DatabaseObject) ASSERT n.dbId IS UNIQUE;
CREATE CONSTRAINT stId_unique IF NOT EXISTS ON (n:DatabaseObject) ASSERT n.stId IS UNIQUE;
CREATE (u:DatabaseObject:ReferenceDatabase {dbId: 2, displayName: 'UniProt', schemaClass: 'ReferenceDatabase'})
CREATE (c:DatabaseObject:ReferenceDatabase {dbId: 114984, displayName: 'ChEBI', schemaClass: 'ReferenceDatabase'})
CREATE (:DatabaseObject:Taxon:Species {dbId: 48887, displayName: 'Homo sapiens', taxId: '9606', schemaClass: 'Species'})
CREATE (:DatabaseObject:Taxon:Species {dbId: 48892, displayName: 'Mus musculus', taxId: '10090', schemaClass: 'Species'})
CREATE (:DatabaseObject:Taxon:Species {dbId: 48895, displayName: 'Rattus norvegicus', taxId: '10116', schemaClass: 'Species'})
CREATE (:DatabaseObject:Event:ReactionLikeEvent:Reaction {dbId: 1000, stId: 'R-HSA-1000', displayName: 'a reaction', schemaClass: 'Reaction'});

// Targets, each the input of the reaction.
MATCH (u:ReferenceDatabase {dbId: 2}), (h:Species {taxId: '9606'}), (r:Reaction {dbId: 1000})
UNWIND [['O43426', 2001, 3001], ['P36544', 2002, 3002], ['Q9NP97', 2003, 3003]] AS row
CREATE (re:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct {dbId: row[1], identifier: row[0], displayName: 'UniProt:' + row[0], schemaClass: 'ReferenceGeneProduct', stId: 'uniprot:' + row[0]})
CREATE (re)-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u)
CREATE (re)-[:species {stoichiometry: 1, order: 1}]->(h)
CREATE (pe:DatabaseObject:PhysicalEntity:GenomeEncodedEntity:EntityWithAccessionedSequence {dbId: row[2], stId: 'R-HSA-' + row[2], displayName: row[0] + ' protein', schemaClass: 'EntityWithAccessionedSequence'})
CREATE (pe)-[:referenceEntity {stoichiometry: 1, order: 1}]->(re)
CREATE (r)-[:input {stoichiometry: 1, order: 1}]->(pe);

// A target through a catalyst alone, with a blank variantIdentifier: its identifier is
// read, as graph-importer reads it (a blank one is no variant).
MATCH (u:ReferenceDatabase {dbId: 2}), (h:Species {taxId: '9606'}), (r:Reaction {dbId: 1000})
CREATE (re:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct {dbId: 2004, identifier: 'Q8NFH8', variantIdentifier: '', displayName: 'UniProt:Q8NFH8', schemaClass: 'ReferenceGeneProduct', stId: 'uniprot:Q8NFH8'})
CREATE (re)-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u)
CREATE (pe:DatabaseObject:PhysicalEntity:GenomeEncodedEntity:EntityWithAccessionedSequence {dbId: 3004, stId: 'R-HSA-3004', displayName: 'Q8NFH8 protein', schemaClass: 'EntityWithAccessionedSequence'})
CREATE (pe)-[:referenceEntity {stoichiometry: 1, order: 1}]->(re)
CREATE (ca:DatabaseObject:CatalystActivity {dbId: 4004, displayName: 'its activity', schemaClass: 'CatalystActivity'})
CREATE (ca)-[:physicalEntity {stoichiometry: 1, order: 1}]->(pe)
CREATE (r)-[:catalystActivity {stoichiometry: 1, order: 1}]->(ca);

// A target that is an isoform already in the graph: matched by its variantIdentifier, so the
// interaction that names it is not given a second O54918-2.
MATCH (u:ReferenceDatabase {dbId: 2}), (m:Species {taxId: '10090'}), (r:Reaction {dbId: 1000})
CREATE (re:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct:ReferenceIsoform {dbId: 2005, identifier: 'O54918', variantIdentifier: 'O54918-2', displayName: 'UniProt:O54918-2', schemaClass: 'ReferenceIsoform', stId: 'uniprot:O54918-2'})
CREATE (re)-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u)
CREATE (re)-[:species {stoichiometry: 1, order: 1}]->(m)
CREATE (pe:DatabaseObject:PhysicalEntity:GenomeEncodedEntity:EntityWithAccessionedSequence {dbId: 3005, stId: 'R-MMU-3005', displayName: 'O54918-2 protein', schemaClass: 'EntityWithAccessionedSequence'})
CREATE (pe)-[:referenceEntity {stoichiometry: 1, order: 1}]->(re)
CREATE (r)-[:output {stoichiometry: 1, order: 1}]->(pe);

// Not a target: no event uses it, so its interaction (with P25962, not in the graph) is not imported.
MATCH (u:ReferenceDatabase {dbId: 2})
CREATE (:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct {dbId: 2006, identifier: 'P12931', displayName: 'UniProt:P12931', schemaClass: 'ReferenceGeneProduct', stId: 'uniprot:P12931'})-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u);

// In the graph but not a target: a partner that is linked to, never made again (its stId is taken).
MATCH (u:ReferenceDatabase {dbId: 2})
CREATE (:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct {dbId: 2010, identifier: 'P49418', displayName: 'UniProt:P49418', schemaClass: 'ReferenceGeneProduct', stId: 'uniprot:P49418'})-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u);

// An identifier and no ReferenceDatabase: skipped, not the end of the selection.
CREATE (:DatabaseObject:ReferenceEntity:ReferenceGeneProduct {dbId: 2009, identifier: 'P99999', displayName: 'orphan', schemaClass: 'ReferenceGeneProduct'});

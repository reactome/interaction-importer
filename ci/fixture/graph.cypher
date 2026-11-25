CREATE (u:DatabaseObject:ReferenceDatabase {dbId: 2, displayName: 'UniProt', schemaClass: 'ReferenceDatabase'})
CREATE (c:DatabaseObject:ReferenceDatabase {dbId: 114984, displayName: 'ChEBI', schemaClass: 'ReferenceDatabase'})
CREATE (h:DatabaseObject:Taxon:Species {dbId: 48887, displayName: 'Homo sapiens', taxId: '9606', schemaClass: 'Species'})
CREATE (m:DatabaseObject:Taxon:Species {dbId: 48892, displayName: 'Mus musculus', taxId: '10090', schemaClass: 'Species'})
CREATE (r:DatabaseObject:Event:ReactionLikeEvent:Reaction {dbId: 1000, stId: 'R-HSA-1000', displayName: 'a reaction', schemaClass: 'Reaction'})
WITH u, h, r
UNWIND [['O43426', 2001, 3001], ['P36544', 2002, 3002], ['Q9NP97', 2003, 3003]] AS row
CREATE (re:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct {dbId: row[1], identifier: row[0], displayName: 'UniProt:' + row[0], schemaClass: 'ReferenceGeneProduct', stId: 'uniprot:' + row[0]})
CREATE (re)-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u)
CREATE (re)-[:species {stoichiometry: 1, order: 1}]->(h)
CREATE (pe:DatabaseObject:PhysicalEntity:GenomeEncodedEntity:EntityWithAccessionedSequence {dbId: row[2], stId: 'R-HSA-' + row[2], displayName: row[0] + ' protein', schemaClass: 'EntityWithAccessionedSequence'})
CREATE (pe)-[:referenceEntity {stoichiometry: 1, order: 1}]->(re)
CREATE (r)-[:input {stoichiometry: 1, order: 1}]->(pe);
// an entity with an identifier and no ReferenceDatabase: skipped, not the end of the selection
CREATE (:DatabaseObject:ReferenceEntity:ReferenceGeneProduct {dbId: 2009, identifier: 'P99999', displayName: 'orphan', schemaClass: 'ReferenceGeneProduct'});
// one no event uses: in the map (a partner can be it) but not a target
MATCH (u:ReferenceDatabase {dbId: 2}) CREATE (:DatabaseObject:ReferenceEntity:ReferenceSequence:ReferenceGeneProduct {dbId: 2010, identifier: 'P49418', displayName: 'UniProt:P49418', schemaClass: 'ReferenceGeneProduct'})-[:referenceDatabase {stoichiometry: 1, order: 1}]->(u);

package org.reactome.server.graph.interactors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ClassUtils;
import org.gk.model.ReactomeJavaConstants;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;
import org.neo4j.graphdb.RelationshipType;
import org.reactome.server.graph.domain.model.ReferenceGeneProduct;
import org.reactome.server.graph.domain.model.ReferenceIsoform;
import org.reactome.server.graph.domain.model.ReferenceMolecule;
import org.reactome.server.graph.domain.model.UndirectedInteraction;
import org.reactome.server.graph.utils.ProgressBarUtils;
import org.reactome.server.graph.utils.TaxonomyHelper;
import org.reactome.server.interactors.IntactParser;
import org.reactome.server.interactors.database.InteractorsDatabase;
import org.reactome.server.interactors.exception.InvalidInteractionResourceException;
import org.reactome.server.interactors.model.Interaction;
import org.reactome.server.interactors.model.Interactor;
import org.reactome.server.interactors.model.InteractorResource;
import org.reactome.server.interactors.service.InteractionService;
import org.reactome.server.interactors.service.InteractorResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.neo4j.driver.Values.parameters;
import static org.reactome.server.graph.utils.FormatUtils.getTimeFormatted;

/**
 * Imports interaction data from the IntAct database.
 * Uses the interactors-core project (https://github.com/reactome-pwp/interactors-core)
 */
public class InteractionImporter {

    private static final Logger importLogger = LoggerFactory.getLogger("import");

    public static final String DBID = "dbId";
    private static final String IDENTIFIER = ReactomeJavaConstants.identifier;
    private static final String VARIANT_IDENTIFIER = ReactomeJavaConstants.variantIdentifier;
    public static final String NAME = "displayName";

    public static final String STOICHIOMETRY = "stoichiometry";
    public static final String ORDER = "order";

    public static Long maxDbId;
    private final Map<Long, Long> dbIds;

    private final TaxonomyHelper taxonomyHelper;

    private static final Long REACTOME_UNIPROT_REFERENCE_DATABASE = 2L;
    private static final Long REACTOME_CHEBI_REFERENCE_DATABASE = 114984L;

    private static Boolean useUserInteractionData;
    private static Boolean isSQLLite;
    private static String userInteractionDataFile;
    private static final String INTERACTION_DATA_TMP_FILE = "./interaction-data.tmp.db";
    private static final Integer QUERIES_OFFSET = 1000;
    private InteractorsDatabase interactorsDatabase;
    private InteractionService interactionService;
    private InteractorResourceService interactorResourceService;

    private Long intActReferenceDatabaseDbId;
    private static final Map<String, Set<Long>> referenceEntityMap = new HashMap<>(); // (UniProt:12345) -> [dbId]
    private static final Map<Long, InteractorResource> interactorResourceMap = new HashMap<>();
    private static final Map<Class<?>, String[]> labelMap = new HashMap<>();

    public InteractionImporter(Transaction tx, String fileName, Boolean isSQLLite) {
        Result maxDbIdResult = tx.run("MATCH (n:DatabaseObject) RETURN max(n.dbId) AS maxDbId");
        Record maxDbIdRecord = maxDbIdResult.single();
        maxDbId = maxDbIdRecord.get("maxDbId").asLong();
        this.dbIds = fetchDbIds(tx);
        this.taxonomyHelper = new TaxonomyHelper(fetchTaxIds(tx));
        useUserInteractionData = fileName != null && !fileName.isEmpty();
        userInteractionDataFile = fileName;
        this.isSQLLite = isSQLLite;
    }

    public void addInteractionData(Transaction tx) {
        long start = System.currentTimeMillis();
        initialise();

        RelationshipType interactor = RelationshipType.withName(ReactomeJavaConstants.interactor);
        RelationshipType referenceDatabase = RelationshipType.withName(ReactomeJavaConstants.referenceDatabase);
        RelationshipType species = RelationshipType.withName(ReactomeJavaConstants.species);

        Map<String, Object> stdRelationshipProp = new HashMap<>();
        stdRelationshipProp.put(STOICHIOMETRY, 1);
        stdRelationshipProp.put(ORDER, 1);

        Long graphImporterUserNode = TrackingObjects.createGraphImporterUserNode(tx);
        intActReferenceDatabaseDbId = TrackingObjects.createIntActReferenceDatabase(dbIds, graphImporterUserNode, tx);
        Long intActReferenceDatabaseNode = dbIds.get(intActReferenceDatabaseDbId);

        Set<Long> addedInteractions = new HashSet<>();
        int addedReferenceEntities = 0;
        Collection<Node> referenceEntities = getTargetReferenceEntities(tx);
        int i = 0; int total = referenceEntities.size();
        for (Node referenceEntity : referenceEntities) {
            ProgressBarUtils.updateProgressBar(++i, total);
            if(i % QUERIES_OFFSET == 0) cleanInteractorsCache();
            final Long a = dbIds.get(referenceEntity.get(DBID).asLong());
            if (a == null) continue;

            String sourceIdentifier = null;
            if (!referenceEntity.get(VARIANT_IDENTIFIER).isNull()) sourceIdentifier = referenceEntity.get(VARIANT_IDENTIFIER).asString();
            else if (!referenceEntity.get(IDENTIFIER).isNull()) sourceIdentifier = referenceEntity.get(IDENTIFIER).asString();
            if (sourceIdentifier != null) {
                String resource = getReferenceDatabaseName(tx, referenceEntity);
                for (Interaction intactInteraction : getIntActInteraction(resource, sourceIdentifier)) {

                    final String targetIdentifier = intactInteraction.getInteractorB().getAcc().trim().split(" ")[0];

                    final Set<Long> targetEntities = referenceEntityMap.get(targetIdentifier);

                    final List<Long> targetNodes = new ArrayList<>();
                    if (targetEntities != null && !targetEntities.isEmpty())  {
                        targetEntities.forEach(t -> {
                            if (dbIds.containsKey(t)) targetNodes.add(dbIds.get(t));
                        });
                    } else {
                        Interactor ib = intactInteraction.getInteractorB();
                        Map<String, Object> toReferenceEntity = createReferenceEntityMap(ib);
                        Long dbId = (Long) toReferenceEntity.get(DBID);
                        Long refDbNode = (Long) toReferenceEntity.remove("referenceDatabaseNode");
                        String[] labels = (String[]) toReferenceEntity.remove("labels");
                        Long b = createNode(tx, toReferenceEntity, labels);
                        TrackingObjects.addCreatedModified(b, graphImporterUserNode, tx);
                        dbIds.put(dbId, b);
                        targetNodes.add(b);
                        referenceEntityMap.computeIfAbsent(targetIdentifier, k -> new HashSet<>()).add(dbId);
                        createRelationship(tx, b, refDbNode, referenceDatabase, stdRelationshipProp);
                        //Adding species relationship when exists
                        Long speciesDbId = taxonomyHelper.getTaxonomyLineage(ib.getTaxid());
                        if (speciesDbId != null) {
                            Long speciesNode = dbIds.get(speciesDbId);
                            if (speciesNode != null) {
                                createRelationship(tx, b, speciesNode, species, stdRelationshipProp);
                                importLogger.info("species " + speciesDbId + " added to " + dbId);
                            }
                        }
                        addedReferenceEntities++;
                    }

                    String sourceName = resource + ":" + sourceIdentifier;
                    String interactionName =  sourceName + " <-> " + targetIdentifier + " (IntAct)";
                    for (Long b : targetNodes) {
                        //Check whether the interaction has been added before
                        if (addedInteractions.contains(intactInteraction.getId())) continue;
                        addedInteractions.add(intactInteraction.getId());

                        //Add interaction instance (UndirectedInteraction)
                        Long dbId = ++maxDbId;
                        Map<String, Object> interaction = createInteractionMap(dbId, interactionName, intactInteraction);
                        long interactionNode = createNode(tx, interaction, getLabels(UndirectedInteraction.class));
                        createRelationship(tx, interactionNode, intActReferenceDatabaseNode, referenceDatabase, stdRelationshipProp);
                        TrackingObjects.addCreatedModified(interactionNode, graphImporterUserNode, tx);
                        dbIds.put(dbId, interactionNode);

                        //Add interaction source (A)
                        Map<String, Object> properties = new HashMap<>();
                        properties.put(STOICHIOMETRY, 1);
                        properties.put(ORDER, 1);
                        createRelationship(tx, interactionNode, a, interactor, properties);

                        //Add interaction target (B)
                        properties.put(ORDER, 2);
                        createRelationship(tx, interactionNode, b, interactor, properties);
                    }
                }
            }
        }

        finalise();
        Long time = System.currentTimeMillis() - start;
        System.out.printf(
                "\n\t%,d interactions and %,d ReferenceEntity objects have been added to the graph (%s). %n",
                addedInteractions.size(),
                addedReferenceEntities,
                getTimeFormatted(time)
        );
    }

    /**
     * Creates a Neo4j node
     * @param tx Neo4j Driver transaction
     * @param props The properties of the node
     * @param labels Labels of the node
     * @return The Neo4j database ID of the created node
     */
    public static long createNode(Transaction tx, Map<String, Object> props, String[] labels) {
        String query = String.format(
                "CREATE (n:%s) SET n = $props RETURN ID(n)", String.join(":", labels));
        Result result = tx.run(query, parameters("props", props));
        Record record = result.single();
        return record.get("ID(n)").asLong();
    }

    /**
     * Create a relationship between two Neo4j nodes
     * @param tx Neo4j Driver transaction
     * @param n1 Source node database ID
     * @param n2 Target node database ID
     * @param type Type of relationship to be created
     * @param props Props to be set on the relationship
     */
    public static void createRelationship(Transaction tx, long n1, long n2, RelationshipType type, Map<String, Object> props) {
        String query = String.format(
                "MATCH (n1:DatabaseObject) WHERE ID(n1) = $n1 " +
                "MATCH (n2:DatabaseObject) WHERE ID(n2) = $n2 " +
                "CREATE (n1)-[r:%s]->(n2) SET r = $props", type.name());
        tx.run(query, parameters("n1", n1, "n2", n2, "props", props));
    }

    private Map<String, Object> createInteractionMap(Long dbId, String name, Interaction interaction){
        String interactionURL = "https://www.ebi.ac.uk/intact/pages/interactions/interactions.xhtml?query=";
        List<String> accession = new ArrayList<>();
        interaction.getInteractionDetailsList().forEach(details -> accession.add(details.getInteractionAc()));
        List<String> pubmeds = interaction.getPubmedIdentifiers();

        Map<String, Object> rtn = new HashMap<>();
        rtn.put(DBID, dbId);
        rtn.put(NAME, name);
        rtn.put("databaseName", "IntAct");
        rtn.put("score", interaction.getIntactScore());
        rtn.put(ReactomeJavaConstants.accession, accession.toArray(new String[0]));
        if (pubmeds != null) rtn.put("pubmed", pubmeds.toArray(new String[0]));
        rtn.put(ReactomeJavaConstants.url, interactionURL + String.join("%20OR%20", accession));
        rtn.put("schemaClass", UndirectedInteraction.class.getSimpleName());
        return rtn;
    }

    private Map<String, Object> createReferenceEntityMap(Interactor interactor){
        InteractorResource resource = getInteractorResource(interactor);
        String identifier = interactor.getAcc().split(" ")[0].trim();
        String rawIdentifier = identifier.contains(":") ? identifier.split(":")[1] : identifier;

        Map<String, Object> rtn = new HashMap<>();
        rtn.put(DBID, ++maxDbId);

        String gn = interactor.getAliasWithoutSpecies(false);
        if (gn != null && !gn.isEmpty()) {
            String[] geneName = new String[1];
            geneName[0] = gn;
            rtn.put("geneName", geneName);
            rtn.put(NAME, identifier + " " + gn);    //Unified to Reactome name
        } else {
            rtn.put(NAME, identifier);               //Unified to Reactome name
        }

        Class<?> schemaClass;
        Long refDbId;
        if (resource.getName().toLowerCase().contains("uniprot")) {
            refDbId = REACTOME_UNIPROT_REFERENCE_DATABASE;
            //displayName added below
            rtn.put(IDENTIFIER, rawIdentifier.split("-")[0]);  //DO NOT MOVE OUTSIDE
            rtn.put(NAME, "UniProt");

            if (rawIdentifier.contains("-")) {
                //for cases like UniProt:O00187-PRO_0000027598 MASP2
                if(rawIdentifier.split("-")[1].contains("PRO")){
                    rtn.put("url", "https://www.uniprot.org/uniprotkb/" + rawIdentifier.split("-")[0] +"/entry#" + rawIdentifier.split("-")[1]);
                }else{
                    rtn.put("url", "https://www.uniprot.org/uniprotkb/" + rawIdentifier + "/entry");
                }
                rtn.put(VARIANT_IDENTIFIER, rawIdentifier);
                //isofromParent //TODO
                schemaClass = ReferenceIsoform.class;
            } else {
                rtn.put("url", "https://www.uniprot.org/uniprotkb/" + rawIdentifier + "/entry");
                schemaClass = ReferenceGeneProduct.class;
            }
        } else if (resource.getName().toLowerCase().contains("chebi")) {
            refDbId = REACTOME_CHEBI_REFERENCE_DATABASE;
            //displayName added below
            rtn.put(IDENTIFIER, rawIdentifier);  //DO NOT MOVE OUTSIDE
            String alias = interactor.getAlias();
            if(alias != null && !alias.isEmpty()) {
                String[] name = new String[1];
                name[0] = alias;
                rtn.put("name", name);
            }
            rtn.put("databaseName", resource.getName());
            rtn.put("url", "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:" + rawIdentifier);
            schemaClass = ReferenceMolecule.class;
        } else {
            resource.setName("IntAct");
            refDbId = intActReferenceDatabaseDbId;
            rtn.put(IDENTIFIER, rawIdentifier);  //DO NOT MOVE OUTSIDE
            rtn.put("databaseName", resource.getName());
            rtn.put("url", "https://www.ebi.ac.uk/intact/query/" + rawIdentifier);
            schemaClass = ReferenceGeneProduct.class;
        }
        if (interactor.getSynonyms() != null && !interactor.getSynonyms().isEmpty()) {
            rtn.put("secondaryIdentifier", interactor.getSynonyms().split("\\$"));
        }
        rtn.put("schemaClass", schemaClass.getSimpleName());

        //These two will be removed from the map
        rtn.put("referenceDatabaseNode", dbIds.get(refDbId));
        rtn.put("labels", getLabels(schemaClass));

        return rtn;
    }

    private void initialise() {
        try {
            System.out.print("\n\nCleaning instances cache...");
            importLogger.info("Cleaning instances cache");
            if (useUserInteractionData) {
                if (isSQLLite) {
                    System.out.print("\rConnecting to the provided interaction data...");
                    importLogger.info("Connecting to the provided interaction data");
                    interactorsDatabase = new InteractorsDatabase(userInteractionDataFile);
                    importLogger.info("Connected to the provided interaction data");
                    System.out.print("\rConnected to the provided interaction data");
                } else {
                    System.out.print("\rConnecting to the provided interaction data...");
                    importLogger.info("Connecting to the provided interaction data");
                    interactorsDatabase = IntactParser.getInteractors(INTERACTION_DATA_TMP_FILE, userInteractionDataFile);
                    importLogger.info("Connected to the provided interaction data");
                    System.out.print("\rConnected to the provided interaction data");
                }
            } else {
                System.out.print("\rRetrieving interaction data...");
                importLogger.info("Retrieving interaction data");
                interactorsDatabase = IntactParser.getInteractors(INTERACTION_DATA_TMP_FILE);
                importLogger.info("Interaction data retrieved");
                System.out.print("\rInteraction data retrieved");
            }
            interactionService = new InteractionService(interactorsDatabase);
            interactorResourceService = new InteractorResourceService(interactorsDatabase);
        } catch (SQLException | IOException e) {
            System.out.println("\rAn error occurred while retrieving the interaction data");
            importLogger.error("An error occurred while retrieving the interaction data", e);
        } catch (Exception e) {
            importLogger.error("An error occurred while cleaning instances cache", e);
        }
    }

    //It seems like the best way of cleaning the cache is to close the connection and connect again
    private void cleanInteractorsCache(){
        try {
            importLogger.trace("Cleaning interactors cache");
            interactorsDatabase.getConnection().close();
            interactorsDatabase = new InteractorsDatabase(useUserInteractionData && isSQLLite ? userInteractionDataFile : INTERACTION_DATA_TMP_FILE);
            interactionService = new InteractionService(interactorsDatabase);
            interactorResourceService = new InteractorResourceService(interactorsDatabase);
            importLogger.trace("Interactors cache cleaned");
        } catch (SQLException e) {
            importLogger.error("An error occurred while reconnecting to the interaction database", e);
        }
    }

    private void finalise() {
        try {
            interactorsDatabase.getConnection().close();
        } catch (SQLException e) {
            importLogger.error(e.getMessage(), e);
        }
        FileUtils.deleteQuietly(new File(INTERACTION_DATA_TMP_FILE));
    }

    /**
     * Returns a list of ReferenceEntity instances that are target for interaction data and at the same
     * time populates the referenceEntityMap with all the (identifier->[ReferenceEntity instance dbId])
     * contained in the database
     *
     * @return a list of ReferenceEntity instances that are target for interaction data
     */
    private Collection<Node> getTargetReferenceEntities(Transaction tx) {
        System.out.print("\rRetrieving interaction data target ReferenceEntity instances...");
        importLogger.info("Retrieving target ReferenceEntity instances");
        Collection<Node> rtn = new HashSet<>();
        try {
            Result result = tx.run("MATCH (n:DatabaseObject:ReferenceEntity) RETURN n");
            while (result.hasNext()) {
                Record record = result.next();
                Node re = record.get("n").asNode();
                if (re != null) {
                    String identifier = null;
                    if (!re.get(VARIANT_IDENTIFIER).isNull()) {
                        identifier = re.get(VARIANT_IDENTIFIER).asString();
                    }
                    if (identifier == null && !re.get(IDENTIFIER).isNull()) {
                        identifier = re.get(IDENTIFIER).asString();
                    }

                    if (identifier != null) {
                        String refDBQuery = "MATCH (n:DatabaseObject {dbId: $dbId})-[r:referenceDatabase]->(m:DatabaseObject:ReferenceDatabase) RETURN m";
                        Result refDBResult = tx.run(refDBQuery, Collections.singletonMap("dbId", re.get(DBID).asLong()));
                        Record refDBRecord = refDBResult.single();
                        Node refDB = refDBRecord.get("m").asNode();
                        identifier = refDB.get(NAME).asString() + ":" + identifier;

                        referenceEntityMap.computeIfAbsent(identifier, k -> new HashSet<>()).add(re.get(DBID).asLong());

                        String referersQuery = "MATCH (n:DatabaseObject)-[r:referenceEntity]->(m:DatabaseObject:ReferenceEntity {dbId: $dbId}) RETURN n";
                        Result referersResult = tx.run(referersQuery, Collections.singletonMap("dbId", re.get(DBID).asLong()));
                        while (referersResult.hasNext()) {
                            Record referersRecord = referersResult.next();
                            Node pe = referersRecord.get("n").asNode();
                            if (isTarget(tx, pe)) {
                                rtn.add(re);
                                break;  //Only one of the referral PhysicalEntities has to be a target for re to be added
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            importLogger.error("An error occurred while retrieving the target ReferenceEntity instances", e);
        }
        System.out.print("\rRetrieving interaction data target ReferenceEntity instances >> Done");
        importLogger.info("Target ReferenceEntity instances retrieved");
        return rtn;
    }

    private boolean isTarget(Transaction tx, Node pe) {
        boolean rtn = false;
        try {
            String query = "MATCH (n:DatabaseObject)-[r:input|output|physicalEntity|diseaseEntity|regulator]->(m:DatabaseObject {dbId: $dbId}) RETURN m LIMIT 1";
            Result result = tx.run(query, Collections.singletonMap("dbId", pe.get(DBID).asLong()));
            rtn = result.hasNext();
        } catch (Exception e) {
            /*nothing here*/
        }
        return rtn;
    }

    private String getReferenceDatabaseName(Transaction tx, Node referenceEntity){
        try {
            String query = "MATCH (n:DatabaseObject:ReferenceEntity {dbId: $dbId})-[r:referenceDatabase]->(m:DatabaseObject:ReferenceDatabase) RETURN m";
            Result result = tx.run(query, Collections.singletonMap("dbId", referenceEntity.get(DBID).asLong()));
            Record record = result.single();
            Node refDatabase = record.get("m").asNode();
            return refDatabase.get(NAME).asString();
        } catch (Exception e) {
            return "undefined";
        }
    }

    private List<Interaction> getIntActInteraction(String resource, String identifier){
        try {
            String target = resource + ":" + identifier;
            return interactionService.getInteractions(target, "static");
        } catch (InvalidInteractionResourceException | SQLException e) {
            return new ArrayList<>();
        }
    }

    private InteractorResource getInteractorResource(Interactor interactor){
        InteractorResource ir = interactorResourceMap.get(interactor.getInteractorResourceId());
        if(ir == null) {
            try {
                ir = interactorResourceService.getAllMappedById().get(interactor.getInteractorResourceId());
                interactorResourceMap.put(interactor.getInteractorResourceId(), ir);
            } catch (SQLException e) {
                //Nothing here
            }
        }
        return ir;
    }

    /**
     * Getting all SimpleNames as neo4j labels, for given class.
     *
     * @param clazz Clazz of object that will result form converting the instance (e.g. Pathway, Reaction)
     * @return Array of Neo4j SchemaClassCount
     */
    public static String[] getLabels(Class<?> clazz) {

        if (!labelMap.containsKey(clazz)) {
            String[] labels = getAllClassNames(clazz);
            labelMap.put(clazz, labels);
            return labels;
        } else {
            return labelMap.get(clazz);
        }
    }

    /**
     * Getting all SimpleNames as neo4j labels, for given class.
     *
     * @param clazz Clazz of object that will result form converting the instance (e.g. Pathway, Reaction)
     * @return Array of Neo4j SchemaClassCount
     */
    private static String[] getAllClassNames(Class<?> clazz) {
        List<?> superClasses = ClassUtils.getAllSuperclasses(clazz);
        List<String> labels = new ArrayList<>();
        labels.add(clazz.getSimpleName());
        for (Object object : superClasses) {
            Class<?> superClass = (Class<?>) object;
            if (!superClass.equals(Object.class)) {
                labels.add(superClass.getSimpleName());
            }
        }
        //noinspection ToArrayCallWithZeroLengthArrayArgument
        return labels.toArray(new String[labels.size()]);
    }

    private static Map<Long, Long> fetchDbIds(Transaction tx) {
        Map<Long, Long> dbIds = new HashMap<>();
        // only DBInfo labeled node has no dbId... this is the only non DatabaseObject
        String query = "MATCH (n:DatabaseObject) RETURN ID(n), n.dbId";
        Result result = tx.run(query);
        while (result.hasNext()) {
            org.neo4j.driver.Record record = result.next();
            dbIds.put(record.get("n.dbId").asLong(), record.get("ID(n)").asLong());
        }

        return dbIds;
    }

    private static Map<Integer, Long> fetchTaxIds(Transaction tx) {
        Map<Integer, Long> taxIdDbId = new HashMap<>();
        // root does not have a taxId
        String query = "MATCH (n:DatabaseObject:Taxon) WHERE n.taxId IS NOT NULL RETURN n.taxId, n.dbId";
        Result result = tx.run(query);
        while (result.hasNext()) {
            Record record = result.next();
            taxIdDbId.put(Integer.parseInt(record.get("n.taxId").asString()), record.get("n.dbId").asLong());
        }
        return taxIdDbId;
    }
}

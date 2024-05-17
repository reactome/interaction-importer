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
    public static final String URL = ReactomeJavaConstants.url;

    public static final String STOICHIOMETRY = "stoichiometry";
    public static final String ORDER = "order";
    public static final String EBI_BASE_URL = "https://www.ebi.ac.uk";

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

    private static final RelationshipType interactor = RelationshipType.withName(ReactomeJavaConstants.interactor);
    private static final RelationshipType referenceDatabase = RelationshipType.withName(ReactomeJavaConstants.referenceDatabase);
    private static final RelationshipType species = RelationshipType.withName(ReactomeJavaConstants.species);
    public static final Map<String, Object> stdRelationshipProp;
    static {
        stdRelationshipProp = new HashMap<>();
        stdRelationshipProp.put(STOICHIOMETRY, 1);
        stdRelationshipProp.put(ORDER, 1);
    }

    public InteractionImporter(Transaction tx, String fileName, Boolean isSQLLite) {
        maxDbId = fetchMaxDbId(tx);
        this.dbIds = fetchDbIds(tx);
        this.taxonomyHelper = new TaxonomyHelper(fetchTaxIds(tx));
        useUserInteractionData = fileName != null && !fileName.isEmpty();
        userInteractionDataFile = fileName;
        this.isSQLLite = isSQLLite;
    }

    private long fetchMaxDbId(Transaction tx) {
        Result maxDbIdResult = tx.run("MATCH (n:DatabaseObject) RETURN max(n.dbId) AS maxDbId");
        Record maxDbIdRecord = maxDbIdResult.single();
        return maxDbIdRecord.get("maxDbId").asLong();
    }

    public void addInteractionData(Transaction tx) {
        long start = System.currentTimeMillis();
        initialise();

        Long graphImporterUserNode = TrackingObjects.createGraphImporterUserNode(tx);
        intActReferenceDatabaseDbId = TrackingObjects.createIntActReferenceDatabase(tx, dbIds, graphImporterUserNode);
        Long intActReferenceDatabaseNode = dbIds.get(intActReferenceDatabaseDbId);

        Set<Long> addedInteractions = new HashSet<>();
        int addedReferenceEntities = 0;
        Collection<Node> referenceEntities = getTargetReferenceEntities(tx);
        int i = 0; int total = referenceEntities.size();
        for (Node referenceEntity : referenceEntities) {
            ProgressBarUtils.updateProgressBar(++i, total);
            if(i % QUERIES_OFFSET == 0) cleanInteractorsCache();
            addedReferenceEntities += processReferenceEntity(tx, referenceEntity, graphImporterUserNode, intActReferenceDatabaseNode, addedInteractions);
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

    private int processReferenceEntity(Transaction tx, Node referenceEntity, Long graphImporterUserNode, Long intActReferenceDatabaseNode, Set<Long> addedInteractions) {
        int addedReferenceEntities = 0;
        final Long sourceReferenceEntity = dbIds.get(referenceEntity.get(DBID).asLong());
        if (sourceReferenceEntity == null) return addedReferenceEntities;

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
                    Map<String, Object> toReferenceEntityMap = createReferenceEntityMap(ib);
                    Long dbId = (Long) toReferenceEntityMap.get(DBID);
                    Long refDbNode = (Long) toReferenceEntityMap.remove("referenceDatabaseNode");
                    String[] labels = (String[]) toReferenceEntityMap.remove("labels");
                    Long targetReferenceEntity = createNode(tx, toReferenceEntityMap, labels);
                    TrackingObjects.addCreatedModified(tx, targetReferenceEntity, graphImporterUserNode);
                    dbIds.put(dbId, targetReferenceEntity);
                    targetNodes.add(targetReferenceEntity);
                    referenceEntityMap.computeIfAbsent(targetIdentifier, k -> new HashSet<>()).add(dbId);
                    createRelationship(tx, targetReferenceEntity, refDbNode, referenceDatabase, stdRelationshipProp);
                    //Adding species relationship when exists
                    Long speciesDbId = taxonomyHelper.getTaxonomyLineage(ib.getTaxid());
                    if (speciesDbId != null) {
                        Long speciesNode = dbIds.get(speciesDbId);
                        if (speciesNode != null) {
                            createRelationship(tx, targetReferenceEntity, speciesNode, species, stdRelationshipProp);
                            importLogger.info("species " + speciesDbId + " added to " + dbId);
                        }
                    }
                    addedReferenceEntities++;
                }

                String sourceName = resource + ":" + sourceIdentifier;
                String interactionName =  sourceName + " <-> " + targetIdentifier + " (IntAct)";
                for (Long targetNode : targetNodes) {
                    //Check whether the interaction has been added before
                    if (addedInteractions.contains(intactInteraction.getId())) continue;
                    addedInteractions.add(intactInteraction.getId());

                    //Add interaction instance (UndirectedInteraction)
                    Long dbId = ++maxDbId;
                    Map<String, Object> interactionMap = createInteractionMap(dbId, interactionName, intactInteraction);
                    long interactionNode = createNode(tx, interactionMap, getLabels(UndirectedInteraction.class));
                    createRelationship(tx, interactionNode, intActReferenceDatabaseNode, referenceDatabase, stdRelationshipProp);
                    TrackingObjects.addCreatedModified(tx, interactionNode, graphImporterUserNode);
                    dbIds.put(dbId, interactionNode);

                    //Add interaction source (A)
                    Map<String, Object> properties = new HashMap<>();
                    properties.put(STOICHIOMETRY, 1);
                    properties.put(ORDER, 1);
                    createRelationship(tx, interactionNode, sourceReferenceEntity, interactor, properties);

                    //Add interaction target (B)
                    properties.put(ORDER, 2);
                    createRelationship(tx, interactionNode, targetNode, interactor, properties);
                }
            }
        }
        return addedReferenceEntities;
    }

    /**
     * Creates a Neo4j node
     * @param tx Neo4j Driver transaction
     * @param props The properties of the node
     * @param labels Labels of the node
     * @return The Neo4j database ID of the created node, or -1 if node creation failed
     */
    public static long createNode(Transaction tx, Map<String, Object> props, String[] labels) {
        String query = "CREATE (n";
        if (labels.length > 0) query += ":" + String.join(":", labels);
        query += ") SET n = $props RETURN ID(n)";

        try {
            Result result = tx.run(query, parameters("props", props));
            Record record = result.single();
            return record.get("ID(n)").asLong();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to create node in graph.");
            System.exit(1);
            return -1;
        }
    }

    /**
     * Create a relationship between two Neo4j nodes
     * @param tx Neo4j Driver transaction
     * @param sourceNode Source node database ID
     * @param targetNode Target node database ID
     * @param type Type of relationship to be created
     * @param props Props to be set on the relationship
     */
    public static void createRelationship(Transaction tx, long sourceNode, long targetNode, RelationshipType type, Map<String, Object> props) {
        String query = "MATCH (n1:DatabaseObject) WHERE ID(n1) = $n1 " +
                "MATCH (n2:DatabaseObject) WHERE ID(n2) = $n2 " +
                "CREATE (n1)-[r:" + type.name() + "]->(n2) SET r = $props";
        tx.run(query, parameters("n1", sourceNode, "n2", targetNode, "props", props));
    }

    /**
     * Create a map of Interaction properties for creating an Interaction node
     * @param dbId dbId of interaction
     * @param name displayName of interaction
     * @param interaction IntAct interaction
     * @return Map of Interaction properties
     */
    private Map<String, Object> createInteractionMap(Long dbId, String name, Interaction interaction){
        String interactionURL = EBI_BASE_URL + "/intact/pages/interactions/interactions.xhtml?query=";
        List<String> accession = new ArrayList<>();
        interaction.getInteractionDetailsList().forEach(details -> accession.add(details.getInteractionAc()));
        List<String> pubmeds = interaction.getPubmedIdentifiers();

        Map<String, Object> interactionMap = new HashMap<>();
        interactionMap.put(DBID, dbId);
        interactionMap.put(NAME, name);
        interactionMap.put("databaseName", "IntAct");
        interactionMap.put("score", interaction.getIntactScore());
        interactionMap.put(ReactomeJavaConstants.accession, accession.toArray(new String[0]));
        if (pubmeds != null) interactionMap.put("pubmed", pubmeds.toArray(new String[0]));
        interactionMap.put(URL, interactionURL + String.join("%20OR%20", accession));
        interactionMap.put("schemaClass", UndirectedInteraction.class.getSimpleName());
        return interactionMap;
    }

    /**
     * Create a map of ReferenceEntity properties for creating a ReferenceEntity node
     * @param interactor IntAct interactor
     * @return Map of ReferenceEntity properties
     */
    private Map<String, Object> createReferenceEntityMap(Interactor interactor){
        InteractorResource resource = getInteractorResource(interactor);
        String identifier = interactor.getAcc().split(" ")[0].trim();
        String rawIdentifier = identifier.contains(":") ? identifier.split(":")[1] : identifier;

        Map<String, Object> _referenceEntityMap = new HashMap<>();
        _referenceEntityMap.put(DBID, ++maxDbId);

        String gn = interactor.getAliasWithoutSpecies(false);
        if (gn != null && !gn.isEmpty()) {
            String[] geneName = new String[1];
            geneName[0] = gn;
            _referenceEntityMap.put("geneName", geneName);
            _referenceEntityMap.put(NAME, identifier + " " + gn);    //Unified to Reactome name
        } else {
            _referenceEntityMap.put(NAME, identifier);               //Unified to Reactome name
        }

        Class<?> schemaClass;
        Long refDbId;
        if (resource.getName().toLowerCase().contains("uniprot")) {
            refDbId = REACTOME_UNIPROT_REFERENCE_DATABASE;
            //displayName added below
            _referenceEntityMap.put(IDENTIFIER, rawIdentifier.split("-")[0]);  //DO NOT MOVE OUTSIDE
            _referenceEntityMap.put("databaseName", "UniProt");

            String UNIPROT_BASE_URL = "https://www.uniprot.org/uniprotkb/";
            if (rawIdentifier.contains("-")) {
                //for cases like UniProt:O00187-PRO_0000027598 MASP2
                if(rawIdentifier.split("-")[1].contains("PRO")){
                    _referenceEntityMap.put(URL, UNIPROT_BASE_URL + rawIdentifier.split("-")[0] +"/entry#" + rawIdentifier.split("-")[1]);
                } else {
                    _referenceEntityMap.put(URL, UNIPROT_BASE_URL + rawIdentifier + "/entry");
                }
                _referenceEntityMap.put(VARIANT_IDENTIFIER, rawIdentifier);
                //isofromParent //TODO
                schemaClass = ReferenceIsoform.class;
            } else {
                _referenceEntityMap.put(URL, UNIPROT_BASE_URL + rawIdentifier + "/entry");
                schemaClass = ReferenceGeneProduct.class;
            }
        } else if (resource.getName().toLowerCase().contains("chebi")) {
            refDbId = REACTOME_CHEBI_REFERENCE_DATABASE;
            //displayName added below
            _referenceEntityMap.put(IDENTIFIER, rawIdentifier);  //DO NOT MOVE OUTSIDE
            String alias = interactor.getAlias();
            if(alias != null && !alias.isEmpty()) {
                String[] name = new String[1];
                name[0] = alias;
                _referenceEntityMap.put(ReactomeJavaConstants.name, name);
            }
            _referenceEntityMap.put("databaseName", resource.getName());
            _referenceEntityMap.put(URL, EBI_BASE_URL + "/chebi/searchId.do?chebiId=CHEBI:" + rawIdentifier);
            schemaClass = ReferenceMolecule.class;
        } else {
            resource.setName("IntAct");
            refDbId = intActReferenceDatabaseDbId;
            _referenceEntityMap.put(IDENTIFIER, rawIdentifier);  //DO NOT MOVE OUTSIDE
            _referenceEntityMap.put("databaseName", resource.getName());
            _referenceEntityMap.put(URL, EBI_BASE_URL + "/intact/query/" + rawIdentifier);
            schemaClass = ReferenceGeneProduct.class;
        }
        if (interactor.getSynonyms() != null && !interactor.getSynonyms().isEmpty()) {
            _referenceEntityMap.put("secondaryIdentifier", interactor.getSynonyms().split("\\$"));
        }
        _referenceEntityMap.put("schemaClass", schemaClass.getSimpleName());

        //These two will be removed from the map
        _referenceEntityMap.put("referenceDatabaseNode", dbIds.get(refDbId));
        _referenceEntityMap.put("labels", getLabels(schemaClass));

        return _referenceEntityMap;
    }

    /**
     * Loads interaction data to be imported into Reactome graph database
     */
    private void initialise() {
        try {
            System.out.print("\n\nCleaning instances cache...");
            importLogger.info("Cleaning instances cache");
            if (useUserInteractionData) {
                loadUserInteractionData();
            } else {
                fetchInteractionData();
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

    /**
     * Loads interaction data provided in userInteractionDataFile
     */
    private void loadUserInteractionData() throws SQLException, IOException {
        System.out.print("\rConnecting to the provided interaction data...");
        importLogger.info("Connecting to the provided interaction data");
        if (isSQLLite) {
            interactorsDatabase = new InteractorsDatabase(userInteractionDataFile);
        } else {
            interactorsDatabase = IntactParser.getInteractors(INTERACTION_DATA_TMP_FILE, userInteractionDataFile);
        }
        importLogger.info("Connected to the provided interaction data");
        System.out.print("\rConnected to the provided interaction data");
    }

    /**
     * Fetch interaction data from ebi
     */
    private void fetchInteractionData() throws SQLException, IOException {
        System.out.print("\rRetrieving interaction data...");
        importLogger.info("Retrieving interaction data");
        interactorsDatabase = IntactParser.getInteractors(INTERACTION_DATA_TMP_FILE);
        importLogger.info("Interaction data retrieved");
        System.out.print("\rInteraction data retrieved");
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

    /**
     * Close database connection and delete temporary SQLite file
     */
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
        Collection<Node> targetReferenceEntities = new HashSet<>();
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

                        String referersQuery = "MATCH (n:DatabaseObject:PhysicalEntity)-[r:referenceEntity]->(m:DatabaseObject:ReferenceEntity {dbId: $dbId}) RETURN n";
                        Result referersResult = tx.run(referersQuery, Collections.singletonMap("dbId", re.get(DBID).asLong()));
                        while (referersResult.hasNext()) {
                            Record referersRecord = referersResult.next();
                            Node physicalEntity = referersRecord.get("n").asNode();
                            if (isTarget(tx, physicalEntity)) {
                                targetReferenceEntities.add(re);
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
        return targetReferenceEntities;
    }

    private boolean isTarget(Transaction tx, Node physicalEntity) {
        boolean isTarget = false;
        try {
            String query = "MATCH (n:DatabaseObject)-[r:input|output|physicalEntity|diseaseEntity|regulator]->(m:DatabaseObject:PhysicalEntity {dbId: $dbId}) RETURN m LIMIT 1";
            Result result = tx.run(query, Collections.singletonMap("dbId", physicalEntity.get(DBID).asLong()));
            isTarget = result.hasNext();
        } catch (Exception e) {
            /*nothing here*/
        }
        return isTarget;
    }

    /**
     * Returns the displayName of the ReferenceDatabase of the provided ReferenceEntity node
     * @param tx Neo4j Driver transaction
     * @param referenceEntity
     * @return The displayName of the ReferenceDatabase related to the ReferenceEntity node, or "undefined" if no such
     * ReferenceDatabase is found in the graph database
     */
    private String getReferenceDatabaseName(Transaction tx, Node referenceEntity){
        try {
            String query = "MATCH (n:DatabaseObject:ReferenceEntity {dbId: $dbId})-[r:referenceDatabase]->(m:DatabaseObject:ReferenceDatabase) RETURN m.displayName";
            Result result = tx.run(query, Collections.singletonMap("dbId", referenceEntity.get(DBID).asLong()));
            Record record = result.single();
            return  record.get("m.displayName").asString();
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

    /**
     * Get a map of DatabaseObject dbIds to Neo4j IDs
     * @param tx Neo4j Driver transaction
     * @return Map of dbIds to Neo4j IDs
     */
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

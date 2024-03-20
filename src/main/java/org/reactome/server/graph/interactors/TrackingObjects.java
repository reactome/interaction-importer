package org.reactome.server.graph.interactors;

import org.gk.model.ReactomeJavaConstants;
import org.neo4j.driver.Transaction;
import org.neo4j.graphdb.RelationshipType;
import org.reactome.server.graph.domain.model.InstanceEdit;
import org.reactome.server.graph.domain.model.Person;
import org.reactome.server.graph.domain.model.ReferenceDatabase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.reactome.server.graph.interactors.InteractionImporter.DBID;
import static org.reactome.server.graph.interactors.InteractionImporter.NAME;
import static org.reactome.server.graph.interactors.InteractionImporter.ORDER;
import static org.reactome.server.graph.interactors.InteractionImporter.STOICHIOMETRY;
import static org.reactome.server.graph.interactors.InteractionImporter.maxDbId;
import static org.reactome.server.graph.interactors.InteractionImporter.createNode;
import static org.reactome.server.graph.interactors.InteractionImporter.createRelationship;

class TrackingObjects {

    private static final RelationshipType author = RelationshipType.withName(ReactomeJavaConstants.author);
    private static final RelationshipType created = RelationshipType.withName(ReactomeJavaConstants.created);
//    private static RelationshipType modified = RelationshipType.withName("modified");

    private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final Map<String, Object> properties;
    static {
        properties = new HashMap<>();
        properties.put(STOICHIOMETRY, 1);
        properties.put(ORDER, 1);
    }

    static Long createIntActReferenceDatabase(Map<Long, Long> dbIds, Long graphImporterUserNode, Transaction tx) {
        Class<?> schemaClass = ReferenceDatabase.class;
        Map<String, Object> intact = new HashMap<>();
        intact.put(DBID, ++maxDbId);
        intact.put(NAME, "IntAct");
        intact.put("name", Collections.singletonList("IntAct").toArray(new String[1]));
        intact.put("schemaClass", schemaClass.getSimpleName());
        intact.put("url", "https://www.ebi.ac.uk/intact");
        intact.put("accessUrl", "https://www.ebi.ac.uk/intact/query/###ID###");
        Long id = createNode(tx, intact, InteractionImporter.getLabels(schemaClass));
        addCreatedModified(id, graphImporterUserNode, tx);
        dbIds.put(maxDbId, id);
        return maxDbId;
    }

    static Long createGraphImporterUserNode(Transaction tx) {
        Class<?> schemaClass = Person.class;
        Map<String, Object> grapUserNode = new HashMap<>();
        grapUserNode.put(DBID, ++maxDbId);
        grapUserNode.put(NAME, "Interactions Importer");
        grapUserNode.put("firstname", "Interactions Importer");
        grapUserNode.put("surname", "Script");
        grapUserNode.put("initial", "AF");
        grapUserNode.put("schemaClass", schemaClass.getSimpleName());
        return createNode(tx, grapUserNode, InteractionImporter.getLabels(schemaClass));
    }

    static void addCreatedModified(Long node, Long graphImporterUserNode, Transaction tx) {
        Long c = createInstanceEditNode(graphImporterUserNode, tx);
        createRelationship(tx, c, node, created, properties);

//        Long m = createInstanceEditNode(graphImporterUserNode, batchInserter);
//        ReactomeBatchImporter.saveRelationship(m, node, modified, properties);
    }

    private static Long createInstanceEditNode(Long graphImporterUserNode, Transaction tx) {
        Class<?> schemaClass = InstanceEdit.class;
        String dateTime = formatter.format(new Date());
        Map<String, Object> instanceEdit = new HashMap<>();
        instanceEdit.put(DBID, ++maxDbId);
        instanceEdit.put(NAME, "Interactions Importer, " + dateTime);
        instanceEdit.put("dateTime", dateTime);
        instanceEdit.put("schemaClass", schemaClass.getSimpleName());
        long id = createNode(tx, instanceEdit, InteractionImporter.getLabels(schemaClass));
        createRelationship(tx, graphImporterUserNode, id, author, properties);
        return id;
    }
}

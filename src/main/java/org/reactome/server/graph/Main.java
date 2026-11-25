package org.reactome.server.graph;

import com.martiansoftware.jsap.*;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.reactome.server.graph.interactors.InteractionImporter;

import java.io.File;

/**
 * @author Florian Korninger (florian.korninger@ebi.ac.uk)
 */
public class Main {
    private static final int EXIT_FAILURE = 1;
    public static void main(String[] args) throws JSAPException {
        JSAPResult config = loadJSAP(args);

        String intactFile = config.getString("intactFile");
        checkIntActFile(intactFile);

        executeInteractionDataImport(config, intactFile);
    }

    /**
     * Load command line arguments into a map
     * @param args Command line arguments
     * @return Map of command line arguments
     */
    private static JSAPResult loadJSAP(String[] args) throws JSAPException {
        SimpleJSAP jsap = new SimpleJSAP(Main.class.getName(), "A tool for importing reactome data import to the neo4j graphDb",
                new Parameter[]{
                        new FlaggedOption(  "host",         JSAP.STRING_PARSER,   "localhost",          JSAP.NOT_REQUIRED, 'h', "host",         "The database host"),
                        new FlaggedOption(  "port",         JSAP.INTEGER_PARSER,  "7687",               JSAP.NOT_REQUIRED, 's', "port",         "The reactome port"),
                        new FlaggedOption(  "name",         JSAP.STRING_PARSER,   JSAP.NO_DEFAULT,      JSAP.NOT_REQUIRED, 'd', "name",         "The graph database to write to (the server's default if not given)"),
                        new FlaggedOption(  "user",         JSAP.STRING_PARSER,   "neo4j",              JSAP.NOT_REQUIRED, 'u', "user",         "The database user"),
                        new FlaggedOption(  "password",     JSAP.STRING_PARSER,   "neo4j",              JSAP.NOT_REQUIRED, 'p', "password",     "The password to connect to the database"),
                        new FlaggedOption(  "intactFile",   JSAP.STRING_PARSER,   JSAP.NO_DEFAULT,          JSAP.NOT_REQUIRED, 'f', "intactFile",   "Path to the interaction data file (downloaded from IntAct if not given, as graph-importer did)"),
                        new QualifiedSwitch("sqlLite",      JSAP.BOOLEAN_PARSER,  JSAP.NO_DEFAULT,          JSAP.NOT_REQUIRED, 'q', "sqlLite",      "Whether the provided file is an SQLite database or a intact-micluster.txt file to be parsed"),
                        new QualifiedSwitch("bar",          JSAP.BOOLEAN_PARSER,  JSAP.NO_DEFAULT,          JSAP.NOT_REQUIRED, 'b', "bar",          "Forces final status")
                }
        );

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.err.println("JSAP parsing failed.");
            System.exit(EXIT_FAILURE);
        }
        return config;
    }

    /**
     * Check that intactFile exists and is a file
     * @param intactFile User provided path to intactFile
     */
    private static void checkIntActFile(String intactFile) {
        //If only intact file is not provided, the interaction-importer will download the interaction data from IntAct
        //The user can specify a location of the file with the interaction content and that will be used
        if(intactFile != null && !intactFile.isEmpty()){
            File f = new File(intactFile);
            if(!f.exists() || f.isDirectory()) {
                System.err.println(intactFile + " does not exist or it is a directory. Please provide the path to the interaction database");
                System.exit(EXIT_FAILURE);
            }
        }
    }

    /**
     * Imports the interactions in one transaction: all of them or, on any failure, none.
     * Not a managed transaction (writeTransaction), which would run the whole import again
     * on a transient error. A graph that already has interactions is refused: run twice, the
     * import would add every interaction, and its IntAct database and Person, a second time.
     */
    private static void executeInteractionDataImport(JSAPResult config, String intactFile) {
        String database = config.getString("name");
        SessionConfig sessionConfig = database == null
                ? SessionConfig.defaultConfig()
                : SessionConfig.forDatabase(database);
        try (Driver driver = getDriver(config); Session session = driver.session(sessionConfig)) {
            long existing = session.readTransaction(tx ->
                    tx.run("MATCH (i:Interaction) RETURN count(i) AS n").single().get("n").asLong());
            if (existing > 0) {
                System.err.printf("The graph already has %,d interactions: not importing them again.%n", existing);
                System.exit(EXIT_FAILURE);
            }
            try (Transaction tx = session.beginTransaction()) {
                InteractionImporter interactionImporter = new InteractionImporter(
                        tx,
                        intactFile,
                        config.getBoolean("sqlLite")
                );
                interactionImporter.addInteractionData(tx);
                tx.commit();
            }
        }
    }

    /**
     * Create a Driver to connect to Neo4j database
     * @param config Program config to create connection to Neo4j
     * @return Neo4j Driver
     */
    private static Driver getDriver(JSAPResult config) {
        String neo4jUser = config.getString("user", "neo4j");
        String neo4jPass = config.getString("password", "neo4j");
        String neo4jHost = config.getString("host", "localhost");
        int neo4jPort = config.getInt("port", 7687);
        String neo4jUri = "bolt://" + neo4jHost + ":" + neo4jPort;

        return GraphDatabase.driver(neo4jUri, AuthTokens.basic(neo4jUser, neo4jPass));
    }
}
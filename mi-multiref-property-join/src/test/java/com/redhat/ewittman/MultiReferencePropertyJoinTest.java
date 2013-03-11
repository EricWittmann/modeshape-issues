/*
 * Copyright 2013 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.ewittman;

import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.RepositoryFactory;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.modeshape.common.collection.Problems;
import org.modeshape.jcr.RepositoryConfiguration;
import org.modeshape.jcr.api.AnonymousCredentials;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;

/**
 * Tests multiple JOINs with multi-reference properties.
 * @author eric.wittmann@redhat.com
 */
public class MultiReferencePropertyJoinTest {

    @Test
    public void test() throws Exception {
        Repository repository = null;
        AnonymousCredentials credentials = new AnonymousCredentials();
        Session session = null;
        Node rootNode = null;

        // Get the ModeShape repo
        Map<String,String> parameters = new HashMap<String,String>();
        URL configUrl = getClass().getResource("inmemory-config.json");
        RepositoryConfiguration config = RepositoryConfiguration.read(configUrl);
        Problems problems = config.validate();
        if (problems.hasErrors()) {
            throw new RepositoryException(problems.toString());
        }
        parameters.put("org.modeshape.jcr.URL",configUrl.toExternalForm());
        for (RepositoryFactory factory : ServiceLoader.load(RepositoryFactory.class)) {
            repository = factory.getRepository(parameters);
            if (repository != null) break;
        }
        if (repository == null) {
            throw new RepositoryException("ServiceLoader could not instantiate JCR Repository");
        }

        // Configure it with our CND
        InputStream is = null;
        session = repository.login(credentials, "default");

        // Register some namespaces.
        NamespaceRegistry namespaceRegistry = session.getWorkspace().getNamespaceRegistry();
        namespaceRegistry.registerNamespace(JCRConstants.SRAMP, JCRConstants.SRAMP_NS);
        namespaceRegistry.registerNamespace(JCRConstants.SRAMP_PROPERTIES, JCRConstants.SRAMP_PROPERTIES_NS);
        namespaceRegistry.registerNamespace(JCRConstants.SRAMP_RELATIONSHIPS, JCRConstants.SRAMP_RELATIONSHIPS_NS);

        NodeTypeManager manager = (NodeTypeManager) session.getWorkspace().getNodeTypeManager();

        // Register the ModeShape S-RAMP node types ...
        is = getClass().getResourceAsStream("multiref-property-join.cnd");
        manager.registerNodeTypes(is, true);
        IOUtils.closeQuietly(is);
        session.logout();

        ////////////////////////////////////////////////
        // Add some artifact nodes.
        ////////////////////////////////////////////////
        session = repository.login(credentials, "default");
        rootNode = session.getRootNode();
        String jcrNodeType = JCRConstants.SRAMP_ + "artifact";
        // Node - Artifact A
        /////////////////////////
        Node artifactA = rootNode.addNode("artifact-a", jcrNodeType);
        artifactA.setProperty("sramp:uuid", "1");
        artifactA.setProperty("sramp:name", "A");
        artifactA.setProperty("sramp:model", "core");
        artifactA.setProperty("sramp:type", "Document");
        // Node - Artifact B
        /////////////////////////
        Node artifactB = rootNode.addNode("artifact-b", jcrNodeType);
        artifactB.setProperty("sramp:uuid", "2");
        artifactB.setProperty("sramp:name", "B");
        artifactB.setProperty("sramp:model", "core");
        artifactB.setProperty("sramp:type", "Document");
        // Node - Artifact C
        /////////////////////////
        Node artifactC = rootNode.addNode("artifact-c", jcrNodeType);
        artifactC.setProperty("sramp:uuid", "3");
        artifactC.setProperty("sramp:name", "C");
        artifactC.setProperty("sramp:model", "core");
        artifactC.setProperty("sramp:type", "Document");
        session.save();

        //////////////////////////////////////////////////////////////////////
        // Add the relationship nodes.  Here's what
        // I'm going for here:
        //    A has relationships to both B and C of type 'relatesTo'
        //    A has a relationship to C of type 'isDocumentedBy'
        //    B has a single relationship to C of type 'covets'
        //    C has no relationships
        //////////////////////////////////////////////////////////////////////
        Node relA_relatesTo = artifactA.addNode("relatesTo", "sramp:relationship");
        relA_relatesTo.setProperty("sramp:type", "relatesTo");
        Value [] targets = new Value[2];
        targets[0] = session.getValueFactory().createValue(artifactB, false);
        targets[1] = session.getValueFactory().createValue(artifactC, false);
        relA_relatesTo.setProperty("sramp:target", targets);

        Node relA_isDocumentedBy = artifactA.addNode("isDocumentedBy", "sramp:relationship");
        relA_isDocumentedBy.setProperty("sramp:type", "isDocumentedBy");
        relA_isDocumentedBy.setProperty("sramp:target", session.getValueFactory().createValue(artifactC, false));

        Node relB_covets = artifactB.addNode("relationship-b-1", "sramp:relationship");
        relB_covets.setProperty("sramp:type", "covets");
        relB_covets.setProperty("sramp:target", session.getValueFactory().createValue(artifactC, false));

        session.save();
        session.logout();


        //////////////////////////////////////////////////////////////////////
        // Now it's time to do some querying.
        //////////////////////////////////////////////////////////////////////
        session = repository.login(credentials, "default");
        // Show that we have the 'relatesTo' relationship set up for Artifact A (with two targets)
        String query = "SELECT relationship.[sramp:target] AS target_jcr_uuid\r\n" +
        		"    FROM [sramp:artifact] AS artifact \r\n" +
        		"    JOIN [sramp:relationship] AS relationship ON ISCHILDNODE(relationship, artifact) \r\n" +
        		"   WHERE artifact.[sramp:name] = 'A'\r\n" +
        		"     AND relationship.[sramp:type] = 'relatesTo'\r\n" +
        		"";
        QueryManager jcrQueryManager = session.getWorkspace().getQueryManager();
        javax.jcr.query.Query jcrQuery = jcrQueryManager.createQuery(query, JCRConstants.JCR_SQL2);
        QueryResult jcrQueryResult = jcrQuery.execute();
        System.out.println("Result 1:");
        System.out.println(jcrQueryResult.toString());
        NodeIterator jcrNodes = jcrQueryResult.getNodes();
        Assert.assertEquals("Expected one (1) node to come back (with two values).", 1, jcrNodes.getSize());
        Node n = jcrNodes.nextNode();
        Set<String> jcr_uuids = new HashSet<String>();
        for (Value value :  n.getProperty("sramp:target").getValues()) {
            System.out.println("  Found JCR UUID: " + value.getString());
            jcr_uuids.add(value.getString());
        }

        // Now show that the UUIDs found above match the jcr:uuid for Artifact B and Artifact C
        query = "SELECT artifact.[jcr:uuid]\r\n" +
        		"  FROM [sramp:artifact] AS artifact \r\n" +
        		" WHERE artifact.[sramp:name] = 'B' OR artifact.[sramp:name] = 'C'\r\n" +
        		"";
        jcrQueryManager = session.getWorkspace().getQueryManager();
        jcrQuery = jcrQueryManager.createQuery(query, JCRConstants.JCR_SQL2);
        jcrQueryResult = jcrQuery.execute();
        System.out.println("\n\nResult 2:");
        System.out.println(jcrQueryResult.toString());
        jcrNodes = jcrQueryResult.getNodes();
        Assert.assertEquals("Expected two (2) nodes to come back.", 2, jcrNodes.getSize());
        Node n1 = jcrNodes.nextNode();
        Node n2 = jcrNodes.nextNode();
        Assert.assertTrue("Expected to find the JCR UUID in jcr_uuids", jcr_uuids.contains(n1.getProperty("jcr:uuid").getString()));
        Assert.assertTrue("Expected to find the JCR UUID in jcr_uuids", jcr_uuids.contains(n2.getProperty("jcr:uuid").getString()));
        System.out.println("Confirmed: the [jcr:uuid] for both Artifact B and Artifact C were found!");

        // OK - so far so good.  Now put it all together in a single query!  Here
        // we are trying to select Artifact B and Artifact C by selecting all Artifacts
        // that Artifatc A has a 'relatesTo' relationship on
        query = "SELECT artifact2.*\r\n" +
        		"   FROM [sramp:artifact] AS artifact1\r\n" +
        		"   JOIN [sramp:relationship] AS relationship1 ON ISCHILDNODE(relationship1, artifact1)\r\n" +
        		"   JOIN [sramp:artifact] AS artifact2 ON relationship1.[sramp:target] = artifact2.[jcr.uuid]\r\n" +
        		"   WHERE artifact1.[sramp:name] = 'A'\r\n" +
        		"    AND relationship1.[sramp:type] = 'relatesTo')\r\n" +
        		"";
        jcrQueryManager = session.getWorkspace().getQueryManager();
        jcrQuery = jcrQueryManager.createQuery(query, JCRConstants.JCR_SQL2);
        jcrQueryResult = jcrQuery.execute();
        System.out.println("\n\nResult 3:");
        System.out.println(jcrQueryResult.toString());
        jcrNodes = jcrQueryResult.getNodes();
        Assert.assertEquals("Expected two (2) nodes (Artifact B and Artifact C) to come back!", 2, jcrNodes.getSize());

        session.logout();

    }

}

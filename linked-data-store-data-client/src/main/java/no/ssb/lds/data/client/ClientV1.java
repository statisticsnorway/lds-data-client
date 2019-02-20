package no.ssb.lds.data.client;

import com.fasterxml.jackson.databind.JsonNode;
import io.aexp.nodes.graphql.GraphQLRequestEntity;
import io.aexp.nodes.graphql.GraphQLResponseEntity;
import io.aexp.nodes.graphql.GraphQLTemplate;
import no.ssb.lds.data.common.model.GSIMComponent;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMRole;
import no.ssb.lds.data.common.model.GSIMType;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;


public class ClientV1 implements Client {

    public static final GraphQLTemplate TEMPLATE = new GraphQLTemplate();
    private final URL graphqlServer;
    private final GraphQLRequestEntity datasetRequest;

    public ClientV1(String url) throws MalformedURLException {
        this.graphqlServer = new URL(url);
        this.datasetRequest = GraphQLRequestEntity.Builder()
                .url(graphqlServer.toString())
                .request("{\n" +
                        "  UnitDataSet {\n" +
                        "    edges {\n" +
                        "      cursor\n" +
                        "      node {\n" +
                        "        name {\n" +
                        "          languageText\n" +
                        "        }\n" +
                        "        description {\n" +
                        "          languageText\n" +
                        "        }\n" +
                        "        unitDataStructure {\n" +
                        "          logicalRecords {\n" +
                        "            edges {\n" +
                        "              node {\n" +
                        "                identifierComponents {\n" +
                        "                  edges {\n" +
                        "                    node {\n" +
                        "                      shortName\n" +
                        "                      isUnique\n" +
                        "                      isComposite\n" +
                        "                      representedVariable {\n" +
                        "                        substantiveValueDomain {\n" +
                        "                          ... on DescribedValueDomain {\n" +
                        "                            dataType\n" +
                        "                          }\n" +
                        "                          ... on EnumeratedValueDomain {\n" +
                        "                            dataType\n" +
                        "                            klassUrl\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }\n" +
                        "                measureComponents {\n" +
                        "                  edges {\n" +
                        "                    node {\n" +
                        "                      shortName\n" +
                        "                      representedVariable {\n" +
                        "                        substantiveValueDomain {\n" +
                        "                          ... on DescribedValueDomain {\n" +
                        "                            dataType\n" +
                        "                          }\n" +
                        "                          ... on EnumeratedValueDomain {\n" +
                        "                            dataType\n" +
                        "                            klassUrl\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }\n" +
                        "                attributeComponents {\n" +
                        "                  edges {\n" +
                        "                    node {\n" +
                        "                      shortName\n" +
                        "                      representedVariable {\n" +
                        "                        substantiveValueDomain {\n" +
                        "                          ... on DescribedValueDomain {\n" +
                        "                            dataType\n" +
                        "                          }\n" +
                        "                          ... on EnumeratedValueDomain {\n" +
                        "                            dataType\n" +
                        "                            klassUrl\n" +
                        "                          }\n" +
                        "                        }\n" +
                        "                      }\n" +
                        "                    }\n" +
                        "                  }\n" +
                        "                }\n" +
                        "              }\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}\n")
                .requestMethod(GraphQLTemplate.GraphQLMethod.QUERY)
                .build();
    }

    /**
     * Returns all the node values in the given connections.
     */
    public static List<JsonNode> getNodes(JsonNode connection) {
        JsonNode edges = connection.get("edges");
        if (edges == null) {
            throw new IllegalArgumentException("the connection " + connection + " did not contain edges");
        }
        if (!edges.isArray()) {
            throw new IllegalArgumentException("the edges in " + connection + " was not an array");
        }
        List<JsonNode> jsonNodes = new ArrayList<>(edges.size());
        for (JsonNode edge : edges) {
            jsonNodes.add(getNode(edge));
        }
        return jsonNodes;
    }

    private static String getCursor(JsonNode edge) {
        JsonNode cursor = edge.get("cursor");
        if (cursor == null) {
            throw new IllegalArgumentException("the edge " + edge + " did not contain cursor");
        }
        return cursor.asText();
    }

    private static JsonNode getNode(JsonNode edge) {
        JsonNode node = edge.get("node");
        if (node == null) {
            throw new IllegalArgumentException("the edge " + edge + " did not contain node");
        }
        return node;
    }

    private static GSIMComponent convertRepresentedVariable(JsonNode node, GSIMRole role) {
        String name = node.get("shortName").asText();
        String typeName = node.get("representedVariable").get("substantiveValueDomain").get("dataType").asText();
        return new GSIMComponent(name, role, GSIMType.valueOf(typeName));
    }

    public GSIMDataset getDataset(String id) {
        GraphQLResponseEntity<JsonNode> responseEntity = TEMPLATE.query(datasetRequest, JsonNode.class);

        // Normalize result.
        JsonNode response = responseEntity.getResponse();
        JsonNode cursor = response.get("UnitDataSet").get("edges").get(0);

        List<GSIMComponent> components = new ArrayList<>();
        GSIMDataset dataset = new GSIMDataset(cursor.get("cursor").asText(), components);

        JsonNode records = cursor.get("node").get("unitDataStructure").get("logicalRecords").get("edges").get(0).get("node");

        List<JsonNode> identifierComponents = getNodes(records.get("identifierComponents"));
        for (JsonNode component : identifierComponents) {
            components.add(convertRepresentedVariable(component, GSIMRole.IDENTIFIER));
        }
        List<JsonNode> measureComponents = getNodes(records.get("measureComponents"));
        for (JsonNode component : measureComponents) {
            components.add(convertRepresentedVariable(component, GSIMRole.MEASURE));
        }
        List<JsonNode> attributeComponents = getNodes(records.get("attributeComponents"));
        for (JsonNode component : attributeComponents) {
            components.add(convertRepresentedVariable(component, GSIMRole.ATTRIBUTE));
        }

        return dataset;
    }

    public void putData(GSIMDataset dataset, ReadableByteChannel data, String mimeType) {

    }

    public GSIMDataset putData(ReadableByteChannel data, String mimeType) {
        return null;
    }

    public Object getData(GSIMDataset dataset, String filter, String order) {
        return null;
    }
}

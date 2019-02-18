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
import java.util.Map;

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

    public GSIMDataset getDataset(String id) {
        GraphQLResponseEntity<JsonNode> responseEntity = TEMPLATE.query(datasetRequest, JsonNode.class);

        // Normalize result.
        JsonNode response = responseEntity.getResponse();
        JsonNode cursor = response.get("UnitDataSet").get("edges").get(0);

        List<GSIMComponent> components = new ArrayList<>();
        GSIMDataset dataset = new GSIMDataset(cursor.get("cursor").asText(), components);

        JsonNode records = cursor.get("node").get("unitDataStructure").get("logicalRecords").get("edges").get(0).get("node");
        JsonNode identifierComponents = records.get("identifierComponents").get("edges");
        for (JsonNode component : identifierComponents) {
            JsonNode node = component.get("node");
            String name = node.get("shortName").asText();
            String typeName = node.get("representedVariable").get("substantiveValueDomain").get("dataType").asText();
            components.add(new GSIMComponent(name, GSIMRole.IDENTIFIER, GSIMType.valueOf(typeName)));
        }
        JsonNode measureComponents = records.get("measureComponents").get("edges");
        for (JsonNode component : measureComponents) {
            JsonNode node = component.get("node");
            String name = node.get("shortName").asText();
            String typeName = node.get("representedVariable").get("substantiveValueDomain").get("dataType").asText();
            components.add(new GSIMComponent(name, GSIMRole.MEASURE, GSIMType.valueOf(typeName)));
        }
        JsonNode attributeComponents = records.get("attributeComponents").get("edges");
        for (JsonNode component : attributeComponents) {
            JsonNode node = component.get("node");
            String name = node.get("shortName").asText();
            String typeName = node.get("representedVariable").get("substantiveValueDomain").get("dataType").asText();
            components.add(new GSIMComponent(name, GSIMRole.ATTRIBUTE, GSIMType.valueOf(typeName)));
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

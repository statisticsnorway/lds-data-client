package no.ssb.lds.data.client;

import io.aexp.nodes.graphql.GraphQLRequestEntity;
import io.aexp.nodes.graphql.GraphQLResponseEntity;
import io.aexp.nodes.graphql.GraphQLTemplate;
import no.ssb.lds.data.common.model.GSIMDataset;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
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
        GraphQLResponseEntity<Map> responseEntity = TEMPLATE.query(datasetRequest, Map.class);
        // Normalize result.
        // TODO: Handle errors.
        Map<String, Object> response = responseEntity.getResponse();
        return null;
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

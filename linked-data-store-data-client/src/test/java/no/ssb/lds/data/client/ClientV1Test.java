package no.ssb.lds.data.client;

import no.ssb.lds.data.common.model.GSIMDataset;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;

class ClientV1Test {

    @Test
    void testGraphql() throws MalformedURLException {
        ClientV1 clientV1 = new ClientV1("http://localhost:9090/graphql");
        GSIMDataset dataset = clientV1.getDataset("todo");
    }
}
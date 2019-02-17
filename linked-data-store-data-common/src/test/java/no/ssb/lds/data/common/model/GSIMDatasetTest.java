package no.ssb.lds.data.common.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static no.ssb.lds.data.common.model.GSIMRole.ATTRIBUTE;
import static no.ssb.lds.data.common.model.GSIMRole.IDENTIFIER;
import static no.ssb.lds.data.common.model.GSIMRole.MEASURE;
import static no.ssb.lds.data.common.model.GSIMType.INTEGER;
import static no.ssb.lds.data.common.model.GSIMType.STRING;

public class GSIMDatasetTest {

    @Test
    public GSIMDataset createDataset() {
        return new GSIMDataset("b9c10b86-5867-4270-b56e-ee7439fe381e",
                List.of(
                        new GSIMComponent("PERSON_ID", IDENTIFIER, STRING),
                        new GSIMComponent("INCOME", MEASURE, INTEGER),
                        new GSIMComponent("GENDER", MEASURE, STRING),
                        new GSIMComponent("MARITAL_STATUS", MEASURE, STRING),
                        new GSIMComponent("MUNICIPALITY", MEASURE, STRING),
                        new GSIMComponent("DATA_QUALITY", ATTRIBUTE, STRING)
                ));
    }
}
package no.ssb.lds.data.common.model;

import java.util.List;

/**
 * Simplified GSIM Dataset representation.
 */
public class GSIMDataset extends GSIMStructure {

    private final String id;

    public GSIMDataset(String id, List<GSIMComponent> components) {
        super(null, components);
        this.id = id;
    }

    public String getId() {
        return id;
    }
}

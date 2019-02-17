package no.ssb.lds.data.common.model;

import java.util.List;

/**
 * Simplified GSIM Dataset representation.
 */
public class GSIMDataset {

    private final String id;
    private final List<GSIMComponent> components;

    public GSIMDataset(String id, List<GSIMComponent> components) {
        this.id = id;
        this.components = components;
    }

    public String getId() {
        return id;
    }

    public List<GSIMComponent> getComponents() {
        return components;
    }
}

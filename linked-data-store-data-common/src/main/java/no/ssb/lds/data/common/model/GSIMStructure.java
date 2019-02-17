package no.ssb.lds.data.common.model;

import java.util.List;

public class GSIMStructure extends GSIMComponent {

    private final List<GSIMComponent> components;

    public GSIMStructure(String name, List<GSIMComponent> components) {
        super(name, GSIMRole.STRUCTURE, GSIMType.ARRAY);
        this.components = components;
    }

    public List<GSIMComponent> getComponents() {
        return components;
    }

}

package no.ssb.lds.data.common.model;

public class GSIMComponent {

    private final String name;

    private final GSIMRole role;

    private final GSIMType type;

    public GSIMComponent(String name, GSIMRole role, GSIMType type) {
        this.name = name;
        this.role = role;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public GSIMRole getRole() {
        return role;
    }

    public GSIMType getType() {
        return type;
    }
}

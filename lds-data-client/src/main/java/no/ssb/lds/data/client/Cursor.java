package no.ssb.lds.data.client;

/**
 * Simple cursor representation.
 *
 * @param <T>
 */
public class Cursor<T> {
    private final T after;
    private final Integer next;

    public Cursor(Integer next, T after) {
        this.after = after;
        this.next = next;
    }

    public T getAfter() {
        return after;
    }

    public Integer getNext() {
        return next;
    }
}

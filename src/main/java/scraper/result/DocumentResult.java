package scraper.result;

public class DocumentResult {
    public final static DocumentResult EMPTY = new DocumentResult(null);

    public final String value;

    public DocumentResult(String value) {
        this.value = value;
    }
}
package scraper.result;

/**
 * Created by Janos Szendi-Varga on 2019. 09. 02.
 */
public class StringResult {
    public final static StringResult EMPTY = new StringResult(null);

    public final String value;

    public StringResult(String value) {
        this.value = value;
    }
}
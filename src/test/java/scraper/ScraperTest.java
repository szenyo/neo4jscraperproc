package scraper;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jsoup.Jsoup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.DependencyResolver.SelectionStrategy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by Janos Szendi-Varga on 2019. 09. 02.
 */

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(Jsoup.class)
public class ScraperTest {

    private static GraphDatabaseService db;

    private static String testUrl = "http://www.mocky.io/v2/5d814df73000004e006995f9";

    @BeforeClass
    public static void setUp() throws Exception {

        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        Procedures proceduresService = ((GraphDatabaseAPI) db)
              .getDependencyResolver().resolveDependency(Procedures.class, SelectionStrategy.FIRST);
        proceduresService.registerProcedure(Scraper.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldGetDocument() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        Result res = db.execute("CALL scraper.getDocument({url}) YIELD value RETURN value",
              map);

        assertTrue(res.hasNext());
        assertEquals(Collections.singletonList("value"), res.columns());
        assertEquals(res.next().get("value"), getTestHtml());
    }

    @Test
    public void shouldReturnElementsBySelect() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);
        map.put("selector", "a[href]");

        Result res = db.execute("CALL scraper.select({url}," +
                    "{selector}) YIELD element RETURN element.attributes" +
                    ".`abs:href` AS col",
              map);

        assertTrue(res.hasNext());
        assertEquals(Collections.singletonList("col"), res.columns());

        List<String> urls = new ArrayList<>();
        while (res.hasNext()) {
            String url = res.next().get("col").toString();
            urls.add(url);
        }
        assertEquals(Arrays.asList("http://www.index.hu", "http://www.index2.hu"), urls);
    }

    @Test
    public void shouldReturnElementsBySelectInHtml() {
        Map<String, Object> map = new HashMap<>();
        map.put("html", getTestHtml());
        map.put("selector", "a[href]");

        Result res = db.execute("CALL scraper.selectInHtml({html}," +
                    "{selector}) YIELD element RETURN element.attributes" +
                    ".`abs:href` AS col",
              map);

        assertTrue(res.hasNext());
        assertEquals(Collections.singletonList("col"), res.columns());

        List<String> urls = new ArrayList<>();
        while (res.hasNext()) {
            String url = res.next().get("col").toString();
            urls.add(url);
        }
        assertEquals(Arrays.asList("http://www.index.hu", "http://www.index2.hu"), urls);
    }

    @Test
    public void shouldReturnTextOfHtmlUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        Result res = db.execute("CALL scraper.getPlainText({url}) YIELD " +
                    "value RETURN value",
              map);

        assertTrue(res.next().get("value").toString().startsWith(" HTML Test" +
              " Page \n" +
              "Testing display of HTML elements"));

        assertTrue(!res.hasNext());
        assertEquals(Collections.singletonList("value"), res.columns());
    }

    @Test
    public void shouldReturnLinkTextOfUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);
        map.put("selector", "a[href]");

        Result res = db.execute("CALL scraper.getPlainText({url}," +
                    "{selector}) " +
                    "YIELD " +
                    "value RETURN value",
              map);

        assertEquals("Index1Index2", res.next().get("value").toString());
        assertTrue(!res.hasNext());
        assertEquals(Collections.singletonList("value"), res.columns());
    }

    @Test
    public void shouldReturnLinksUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        Result res = db.execute("CALL scraper.getLinks({url}) YIELD " +
                    "element RETURN element.attributes.`abs:href` AS col",
              map);
        assertTrue(res.hasNext());
        assertEquals(Collections.singletonList("col"), res.columns());

        List<String> urls = new ArrayList<>();
        while (res.hasNext()) {
            String url = res.next().get("col").toString();
            urls.add(url);
        }
        assertEquals(Arrays.asList("http://www.index.hu", "http://www.index2.hu"), urls);
    }

    @Test
    public void shouldReturnMediaLinksUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        Result res = db.execute("CALL scraper.getMediaLinks({url}) YIELD" +
                    " " +
                    "element RETURN element.attributes.src AS col",
              map);
        assertTrue(res.hasNext());
        assertEquals(Collections.singletonList("col"), res.columns());

        List<String> urls = new ArrayList<>();
        while (res.hasNext()) {
            String url = res.next().get("col").toString();
            urls.add(url);
        }
        assertEquals(Arrays.asList("https://www.google" +
              ".hu/images/branding/googlelogo/2x/googlelogo_color_120x44dp.png"), urls);
    }

    private String getTestHtml() {
        StringBuilder contentBuilder = new StringBuilder();
        try {
            BufferedReader in = new BufferedReader(new FileReader(
                  "src/test/resources/test.html"));
            String str;
            while ((str = in.readLine()) != null) {
                contentBuilder.append(str);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    }
}

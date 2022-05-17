package scraper;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.jsoup.Jsoup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.reflect.io.File;

/**
 * Created by Janos Szendi-Varga on 2019. 09. 02.
 */

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(Jsoup.class)
public class ScraperTest {

    private static GraphDatabaseService db;

    private static String testUrl = "http://www.mocky.io/v2/5d814df73000004e006995f9";

    private static DatabaseManagementService managementService;
    private static DatabaseManager<?> databaseManager;

    @BeforeClass
    public static void setUp() throws Exception {
        Path tempPath = Files.createTempDirectory("neo4j");
        managementService = new TestDatabaseManagementServiceBuilder(tempPath.toAbsolutePath())
                .setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes( 128 ) )
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        databaseManager = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( DatabaseManager.class );

        GlobalProcedures globalProcedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        globalProcedures.registerProcedure(Scraper.class);
    }

    @AfterClass
    public static void tearDown() {
        managementService.shutdown();
    }

    @Test
    public void shouldGetDocument() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        db.executeTransactionally("CALL scraper.getDocument($url) YIELD value RETURN value",
              map, (res) -> {
                    assertTrue(res.hasNext());
                    assertEquals(Collections.singletonList("value"), res.columns());
                    assertEquals(res.next().get("value"), getTestHtml());
                    return null;
                });

    }

    @Test
    public void shouldReturnElementsBySelect() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);
        map.put("selector", "a[href]");

        db.executeTransactionally("CALL scraper.select($url," +
                    "$selector) YIELD element RETURN element.attributes" +
                    ".`abs:href` AS col", map,
                (res) -> {
                    assertTrue(res.hasNext());
                    assertEquals(Collections.singletonList("col"), res.columns());

                    List<String> urls = new ArrayList<>();
                    while (res.hasNext()) {
                        String url = res.next().get("col").toString();
                        urls.add(url);
                    }
                    assertEquals(Arrays.asList("http://www.index.hu", "http://www.index2.hu"), urls);
                    return null;
                });

    }

    @Test
    public void shouldReturnElementsBySelectInHtml() {
        Map<String, Object> map = new HashMap<>();
        map.put("html", getTestHtml());
        map.put("selector", "a[href]");

        db.executeTransactionally("CALL scraper.selectInHtml($html," +
                    "$selector) YIELD element RETURN element.attributes" +
                    ".`abs:href` AS col",
              map, (res) -> {
                    assertTrue(res.hasNext());
                    assertEquals(Collections.singletonList("col"), res.columns());

                    List<String> urls = new ArrayList<>();
                    while (res.hasNext()) {
                        String url = res.next().get("col").toString();
                        urls.add(url);
                    }
                    assertEquals(Arrays.asList("http://www.index.hu", "http://www.index2.hu"), urls);
                    return null;
                });

    }

    @Test
    public void shouldReturnTextOfHtmlUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        db.executeTransactionally("CALL scraper.getPlainText($url) YIELD " +
                    "value RETURN value",
              map, (res) -> {
                    assertTrue(res.next().get("value").toString().startsWith(" HTML Test" +
                            " Page \n" +
                            "Testing display of HTML elements"));

                    assertTrue(!res.hasNext());
                    assertEquals(Collections.singletonList("value"), res.columns());
                    return null;
                });

    }

    @Test
    public void shouldReturnLinkTextOfUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);
        map.put("selector", "a[href]");

        db.executeTransactionally("CALL scraper.getPlainText($url," +
                    "$selector) " +
                    "YIELD " +
                    "value RETURN value",
              map, (res) -> {
                    assertEquals("Index1Index2", res.next().get("value").toString());
                    assertTrue(!res.hasNext());
                    assertEquals(Collections.singletonList("value"), res.columns());
                    return null;
                });

    }

    @Test
    public void shouldReturnLinksUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        db.executeTransactionally("CALL scraper.getLinks($url) YIELD " +
                    "element RETURN element.attributes.`abs:href` AS col",
              map, (res) -> {
                    assertTrue(res.hasNext());
                    assertEquals(Collections.singletonList("col"), res.columns());

                    List<String> urls = new ArrayList<>();
                    while (res.hasNext()) {
                        String url = res.next().get("col").toString();
                        urls.add(url);
                    }
                    assertEquals(Arrays.asList("http://www.index.hu", "http://www.index2.hu"), urls);
                    return null;
                });
    }

    @Test
    public void shouldReturnMediaLinksUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", testUrl);

        db.executeTransactionally("CALL scraper.getMediaLinks($url) YIELD" +
                    " " +
                    "element RETURN element.attributes.src AS col",
              map, (res) -> {
                    assertTrue(res.hasNext());
                    assertEquals(Collections.singletonList("col"), res.columns());
                    List<String> urls = new ArrayList<>();
                    while (res.hasNext()) {
                        String url = res.next().get("col").toString();
                        urls.add(url);
                    }
                    assertEquals(Arrays.asList("https://www.google" +
                            ".hu/images/branding/googlelogo/2x/googlelogo_color_120x44dp.png"), urls);
                    return null;
                });

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

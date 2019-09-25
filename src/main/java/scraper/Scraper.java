package scraper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import scraper.result.JsoupElementResult;
import scraper.result.StringResult;
import scraper.util.CustomHtmlToPlainText;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by Janos Szendi-Varga on 2019. 09. 02.
 */
public class Scraper {

    final static int TIMEOUT = 500;
    public static final String USERAGENT = "Mozilla";
    public static final boolean IGNORE_ERRORS = false;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI dbAPI;


    @Procedure
    @Description("scraper.getDocument(url) YIELD value - " +
          "Return the content of an url")
    public Stream<StringResult> getDocument(@Name("url") String url) throws IOException {

        URL urlObject = new URL(url);
        URLConnection conn = urlObject.openConnection();
        InputStream is = conn.getInputStream();

        String result = new BufferedReader(new InputStreamReader(is))
              .lines().collect(Collectors.joining("\n"));

        if (result.length() == 0) {
            return Stream.of(StringResult.EMPTY);
        } else {
            return Stream.of(new StringResult(result));
        }
    }

    @Procedure
    @Description("scraper.select(url,selector) YIELD element - " +
          "Find elements that match the Selector CSS query, with this element as the starting context.")
    public Stream<JsoupElementResult> select(@Name("url") String url,
          @Name("selector") String selector) throws IOException {
        Document doc = getDoc(url);

        return getResult(doc, selector).stream();
    }

    @Procedure
    @Description("scraper.selectInHtml(html,selector) YIELD element - " +
          "Find elements that match the Selector CSS query, with this element as the starting context.")
    public Stream<JsoupElementResult> selectInHtml(@Name("html") String html,
          @Name("selector") String selector) {
        Document doc = Jsoup.parseBodyFragment(html);

        return getResult(doc, selector).stream();
    }

    @Procedure
    @Description("scraper.getLinks(url) YIELD element - " +
          "Get link elements from an url.")
    public Stream<JsoupElementResult> getLinks(@Name("url") String url) throws IOException {
        Document doc = getDoc(url);

        return getResult(doc, "a[href]").stream();
    }

    @Procedure
    @Description("scraper.getLinksInHtml(html) YIELD element - " +
          "Get link elements from a html.")
    public Stream<JsoupElementResult> getLinksInHtml(@Name("html") String html) {
        Document doc = Jsoup.parseBodyFragment(html);

        return getResult(doc, "a[href]").stream();
    }

    @Procedure
    @Description("scraper.getMediaLinks(url) YIELD element - " +
          "Get media link elements.")
    public Stream<JsoupElementResult> getMediaLinks(@Name("url") String url) throws IOException {
        Document doc = getDoc(url);

        return getResult(doc, "[src]").stream();
    }

    @Procedure
    @Description("scraper.getMediaLinksInHtml(html) YIELD element - " +
          "Get media link elements.")
    public Stream<JsoupElementResult> getMediaLinksInHtml(@Name("html") String html) {
        Document doc = Jsoup.parseBodyFragment(html);

        return getResult(doc, "[src]").stream();
    }

    @Procedure
    @Description("scraper.getPlainText(url,selector) YIELD value - " +
          "Get plain text version of a given page.")
    public Stream<StringResult> getPlainText(@Name("url") String url, @Name(value = "selector", defaultValue = "") String selector) {
        StringBuilder plainText = new StringBuilder();
        try {
            Document doc = getDoc(url);
            CustomHtmlToPlainText formatter = new CustomHtmlToPlainText();
            if (!selector.equals("")) {
                Elements elements = doc.select(selector);
                for (Element element : elements) {
                    plainText.append(formatter.getPlainText(element));
                }
            } else {
                plainText.append(formatter.getPlainText(doc));
            }
        } catch (Exception e) {
            Stream.of(StringResult.EMPTY);
        }

        if (plainText.length() == 0) {
            return Stream.of(StringResult.EMPTY);
        } else {
            return Stream.of(new StringResult(plainText.toString()));
        }

    }

    @Procedure
    @Description("scraper.getPlainTextInHtml(url,selector) YIELD value - " +
          "Get plain text version of a given page.")
    public Stream<StringResult> getPlainTextInHtml(@Name("html") String html, @Name(value = "selector", defaultValue = "") String selector) {
        StringBuilder plainText = new StringBuilder();
        Document doc = Jsoup.parseBodyFragment(html);
        CustomHtmlToPlainText formatter = new CustomHtmlToPlainText();
        if (!selector.equals("")) {
            Elements elements = doc.select(selector);
            for (Element element : elements) {
                plainText.append(formatter.getPlainText(element));
            }
        } else {
            plainText.append(formatter.getPlainText(doc));
        }
        if (plainText.length() == 0) {
            return Stream.of(StringResult.EMPTY);
        } else {
            return Stream.of(new StringResult(plainText.toString()));
        }
    }

    @Procedure
    @Description("scraper.getElementById(url,id) YIELD element - "
          + "Find an element by ID, including or under this element.")
    public Stream<JsoupElementResult> getElementById(@Name("url") String url, @Name("id") String id)
          throws IOException {
        Document doc = getDoc(url);
        Element element = doc.getElementById(id);
        return Stream.of(new JsoupElementResult(url, element));
    }

    @Procedure
    @Description("scraper.getElementByIdInHtml(html,id) YIELD element - " +
          "Find an element by ID, including or under this element.")
    public Stream<JsoupElementResult> getElementByIdInHtml(@Name("html") String html, @Name("id") String id) {
        Document doc = Jsoup.parseBodyFragment(html);
        Element element = doc.getElementById(id);
        return Stream.of(new JsoupElementResult(null, element));
    }

    @Procedure
    @Description("scraper.getElementsByTag(url,tag) YIELD element - "
          + "Finds elements, including and recursively under this element, with the specified tag name.")
    public Stream<JsoupElementResult> getElementsByTag(@Name("url") String url, @Name("tag") String tag) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByTag(tag);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByTagInHtml(html,tag) YIELD element - "
          + "Finds elements, including and recursively under this element, with the specified tag name.")
    public Stream<JsoupElementResult> getElementsByTagInHtml(@Name("html") String html, @Name("tag") String tag) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByTag(tag);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByClass(url,className) YIELD element - "
          + "Find elements that have this class, including or under this element.")
    public Stream<JsoupElementResult> getElementsByClass(@Name("url") String url,
          @Name("className") String className) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByClass(className);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByClassInHtml(html,className) YIELD element - " +
          "Find elements that have this class, including or under this element.")
    public Stream<JsoupElementResult> getElementsByClassInHtml(@Name("html") String html, @Name("className") String className) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByClass(className);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttribute(url,key) YIELD element - "
          + "Find elements that have a named attribute set.")
    public Stream<JsoupElementResult> getElementsByAttribute(@Name("url") String url, @Name("key") String key) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttribute(key);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeInHtml(html,attribute) YIELD element - " +
          "Find elements that have a named attribute set.")
    public Stream<JsoupElementResult> getElementsByAttributeInHtml(@Name("html") String html, @Name("key") String key) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttribute(key);

        return getResult(doc, elements).stream();

    }

    @Procedure
    @Description("scraper.getElementsByAttributeStarting(url,keyPrefix) YIELD element - "
          + "Find elements that have an attribute name starting with the supplied prefix. Use data- to find elements that have HTML5 datasets.")
    public Stream<JsoupElementResult> getElementsByAttributeStarting(@Name("url") String url, @Name("keyPrefix") String keyPrefix) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttributeStarting(keyPrefix);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeStartingInHtml(html,keyPrefix) YIELD element - " +
          "Find elements that have an attribute name starting with the supplied prefix. Use data- to find elements that have HTML5 datasets.")
    public Stream<JsoupElementResult> getElementsByAttributeStartingInHtml(@Name("html") String html, @Name("keyPrefix") String keyPrefix) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeStarting(keyPrefix);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeValue(url,key,value) YIELD element - "
          + "Find elements that have an attribute with the specific value.")
    public Stream<JsoupElementResult> getElementsByAttributeValue(@Name("url") String url, @Name("key") String key, @Name("value") String value) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttributeValue(key, value);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeValueInHtml(html,key,value) YIELD element - "
          + "Find elements that have an attribute with the specific value.")
    public Stream<JsoupElementResult> getElementsByAttributeValueInHtml(@Name("html") String html, @Name("key") String key, @Name("value") String value) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeValue(key, value);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeValueContaining(url,key,match) YIELD element - "
          + "Find elements that have attributes whose value contains the match string.")
    public Stream<JsoupElementResult> getElementsByAttributeValueContaining(@Name("url") String url, @Name("key") String key, @Name("match") String match) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttributeValueContaining(key, match);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description(
          "scraper.getElementsByAttributeValueContainingInHtml(html,key,match) YIELD element - "
                + "Find elements that have attributes whose value contains the match string.")
    public Stream<JsoupElementResult> getElementsByAttributeValueContainingInHtml(@Name("html") String html, @Name("key") String key, @Name("match") String match) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeValueContaining(key, match);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description(
          "scraper.getElementsByAttributeValueEnding(url,key,valueSuffix) YIELD element - "
                + "Find elements that have attributes that end with the value suffix.")
    public Stream<JsoupElementResult> getElementsByAttributeValueEnding(@Name("url") String url, @Name("key") String key, @Name("valueSuffix") String valueSuffix) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttributeValueEnding(key, valueSuffix);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description(
          "scraper.getElementsByAttributeValueEndingInHtml(html,key,valueSuffix) YIELD element - "
                + "Find elements that have attributes that end with the value suffix.")
    public Stream<JsoupElementResult> getElementsByAttributeValueEndingInHtml(@Name("html") String html, @Name("key") String key, @Name("valueSuffix") String valueSuffix) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeValueEnding(key, valueSuffix);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeValueMatching(url,key,regex) YIELD element - "
          + "Find elements that have attributes whose values match the supplied regular expression.")
    public Stream<JsoupElementResult> getElementsByAttributeValueMatching(@Name("url") String url, @Name("key") String key, @Name("regex") String regex) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttributeValueMatching(key, regex);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description(
          "scraper.getElementsByAttributeValueMatchingInHtml(html,key,regex) YIELD element - "
                + "Find elements that have attributes whose values match the supplied regular expression.")
    public Stream<JsoupElementResult> getElementsByAttributeValueMatchingInHtml(@Name("html") String html, @Name("key") String key, @Name("regex") String regex) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeValueMatching(key, regex);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeValueNot(url,key,value) YIELD element - "
          + "Find elements that either do not have this attribute, or have it with a different value.")
    public Stream<JsoupElementResult> getElementsByAttributeValueNot(@Name("url") String url, @Name("key") String key, @Name("value") String value) throws IOException {
        Document doc = getDoc(url);

        Elements elements = doc.getElementsByAttributeValueNot(key, value);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByAttributeValueNotInHtml(html,key,value) YIELD element - "
          + "Find elements that either do not have this attribute, or have it with a different value.")
    public Stream<JsoupElementResult> getElementsByAttributeValueNotInHtml(@Name("html") String html, @Name("key") String key, @Name("value") String value) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeValueNot(key, value);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description(
          "scraper.getElementsByAttributeValueStarting(url,key,valuePrefix) YIELD element - "
                + "Find elements that have attributes that start with the value prefix.")
    public Stream<JsoupElementResult> getElementsByAttributeValueStarting(@Name("url") String url, @Name("key") String key, @Name("valuePrefix") String valuePrefix) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByAttributeValueStarting(key, valuePrefix);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description(
          "scraper.getElementsByAttributeValueStartingInHtml(html,key,valuePrefix) YIELD element - "
                + "Find elements that have attributes that start with the value prefix.")
    public Stream<JsoupElementResult> getElementsByAttributeValueStartingInHtml(@Name("html") String html, @Name("key") String key, @Name("valuePrefix") String valuePrefix) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByAttributeValueStarting(key, valuePrefix);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByIndexEquals(url,index) YIELD element - "
          + "Find elements whose sibling index is equal to the supplied index.")
    public Stream<JsoupElementResult> getElementsByIndexEquals(@Name("url") String url, @Name("index") String index) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByIndexEquals(Integer.parseInt(index));

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByIndexEqualsInHtml(html,index) YIELD element - "
          + "Find elements whose sibling index is equal to the supplied index.")
    public Stream<JsoupElementResult> getElementsByIndexEqualsInHtml(@Name("html") String html, @Name("index") String index) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByIndexEquals(Integer.parseInt(index));

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByIndexGreaterThan(url,index) YIELD element - "
          + "Find elements whose sibling index is greater than the supplied index.")
    public Stream<JsoupElementResult> getElementsByIndexGreaterThan(@Name("url") String url, @Name("index") String index) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByIndexGreaterThan(Integer.parseInt(index));

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByIndexGreaterThanInHtml(html,index) YIELD element - "
          + "Find elements whose sibling index is greater than the supplied index.")
    public Stream<JsoupElementResult> getElementsByIndexGreaterThanInHtml(@Name("html") String html, @Name("index") String index) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByIndexGreaterThan(Integer.parseInt(index));

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByIndexLessThan(url,index) YIELD element - "
          + "Find elements whose sibling index is less than the supplied index.")
    public Stream<JsoupElementResult> getElementsByIndexLessThan(@Name("url") String url, @Name("index") String index) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsByIndexLessThan(Integer.parseInt(index));

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsByIndexLessThanInHtml(html,index) YIELD element - "
          + "Find elements whose sibling index is less than the supplied index.")
    public Stream<JsoupElementResult> getElementsByIndexLessThanInHtml(@Name("html") String html, @Name("index") String index) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsByIndexLessThan(Integer.parseInt(index));

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsContainingOwnText(url,searchText) YIELD element - "
          + "Find elements that directly contain the specified string.")
    public Stream<JsoupElementResult> getElementsContainingOwnText(@Name("url") String url, @Name("searchText") String searchText) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsContainingOwnText(searchText);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsContainingOwnTextInHtml(html,searchText) YIELD element - "
          + "Find elements that directly contain the specified string.")
    public Stream<JsoupElementResult> getElementsContainingOwnTextInHtml(@Name("html") String html, @Name("searchText") String searchText) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsContainingOwnText(searchText);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsContainingText(url,searchText) YIELD element - "
          + "Find elements that contain the specified string.")
    public Stream<JsoupElementResult> getElementsContainingText(@Name("url") String url, @Name("searchText") String searchText) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsContainingText(searchText);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsContainingTextInHtml(html,searchText) YIELD element - "
          + "Find elements that contain the specified string.")
    public Stream<JsoupElementResult> getElementsContainingTextInHtml(@Name("html") String html, @Name("searchText") String searchText) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsContainingText(searchText);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsMatchingOwnText(url,regex) YIELD element - "
          + "Find elements whose text matches the supplied regular expression.")
    public Stream<JsoupElementResult> getElementsMatchingOwnText(@Name("url") String url, @Name("regex") String regex) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsMatchingOwnText(regex);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsMatchingOwnTextInHtml(html,pattern) YIELD element - "
          + "Find elements whose text matches the supplied regular expression.")
    public Stream<JsoupElementResult> getElementsMatchingOwnTextInHtml(@Name("html") String html, @Name("regex") String regex) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsMatchingOwnText(regex);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsMatchingText(url,pattern) YIELD element - "
          + "Find elements whose text matches the supplied regular expression.")
    public Stream<JsoupElementResult> getElementsMatchingText(@Name("url") String url, @Name("regex") String regex) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getElementsMatchingText(regex);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getElementsContainingTextInHtml(html,pattern) YIELD element - "
          + "Find elements whose text matches the supplied regular expression.")
    public Stream<JsoupElementResult> getElementsMatchingTextInHtml(@Name("html") String html, @Name("regex") String regex) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getElementsMatchingText(regex);

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getAllElements(url) YIELD element - "
          + "Find all elements under this element (including self, and children of children).")
    public Stream<JsoupElementResult> getAllElements(@Name("url") String url) throws IOException {
        Document doc = getDoc(url);
        Elements elements = doc.getAllElements();

        return getResult(doc, elements).stream();
    }

    @Procedure
    @Description("scraper.getAllElementsInHtml(html) YIELD element - "
          + "Find all elements under this element (including self, and children of children).")
    public Stream<JsoupElementResult> getAllElementsInHtml(@Name("html") String html) {
        Document doc = Jsoup.parseBodyFragment(html);
        Elements elements = doc.getAllElements();

        return getResult(doc, elements).stream();
    }

    private List<JsoupElementResult> getResult(Document doc, String selector) {
        List<JsoupElementResult> list = new ArrayList<>();
        Elements elements = doc.select(selector);
        for (Element element : elements) {
            list.add(new JsoupElementResult(doc.baseUri(), element));
        }
        return list;
    }

    private List<JsoupElementResult> getResult(Document doc, Elements elements) {
        List<JsoupElementResult> list = new ArrayList<>();
        for (Element element : elements) {
            list.add(new JsoupElementResult(doc.baseUri(), element));
        }
        return list;
    }

    private Document getDoc(@Name("url") String url) throws IOException {
        return Jsoup.connect(url).userAgent(USERAGENT).ignoreHttpErrors(IGNORE_ERRORS)
              .timeout(TIMEOUT)
              .get();
    }
}

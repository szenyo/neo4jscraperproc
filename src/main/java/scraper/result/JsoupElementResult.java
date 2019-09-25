package scraper.result;

import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Element;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Janos Szendi-Varga on 2019. 09. 02.
 */
public class JsoupElementResult {
    
    public Map element = new HashMap();

    public JsoupElementResult(String url, Element jsoupElement) {
        element.put("url",url);
        element.put("text", jsoupElement.text());
        element.put("html", jsoupElement.html());
        element.put("outerHtml", jsoupElement.outerHtml());
        element.put("data", jsoupElement.data());
        element.put("tagName", jsoupElement.tagName());
        element.put("id", jsoupElement.id());
        element.put("className", jsoupElement.className());
        element.put("classNames",jsoupElement.classNames());

        Map attributes = new HashMap();
        Iterator<Attribute> it = jsoupElement.attributes().iterator();
        while (it.hasNext()) {
            Attribute attr = it.next();
            attributes.put(attr.getKey(), attr.getValue());
            if(attr.getKey().equals("href")){
                attributes.put("abs:"+attr.getKey(), jsoupElement.attr
                        ("abs:href"));
            }
        }
        element.put("attributes",attributes);

    }
}
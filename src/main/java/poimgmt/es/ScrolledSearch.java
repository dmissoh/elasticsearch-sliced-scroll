package poimgmt.es;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * TODO {Purpose of This Class}
 *
 * @author MaibornWolff GmbH
 * @version 0.1
 * @since 25.10.18
 */
public class ScrolledSearch {

  int scrollSize = 1000;
  TimeValue scrollTimeout = new TimeValue(60 * 1000);

  RestHighLevelClient client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200, "http")));

  public static void main(String[] args) {
    System.out.println("ScrolledSearch...");
    long start = System.currentTimeMillis();
    ScrolledSearch testClient = new ScrolledSearch();
    try {
      testClient.slicedScrollSearch();
    } catch (IOException e) {
      e.printStackTrace();
    }
    long end = System.currentTimeMillis();
    System.out.println("ScrolledSearch-Time: " + (end - start));
  }

  private void slicedScrollSearch() throws IOException {
    final Scroll scroll;
    scroll = new Scroll(scrollTimeout);
    SearchRequest searchRequest = new SearchRequest("dcs");
    searchRequest.scroll(scroll);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.query(geoDistanceQuery("location").point(52.377498, 4.86902).distance(300, DistanceUnit.KILOMETERS));
    searchSourceBuilder.size(scrollSize);
    searchRequest.source(searchSourceBuilder);

    int total = 0;
    SearchResponse searchResponse = client.search(searchRequest);
    String scrollId = searchResponse.getScrollId();
    SearchHit[] searchHits = searchResponse.getHits().getHits();
    total += searchHits.length;

    while (searchHits != null && searchHits.length > 0) {
      SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
      scrollRequest.scroll(scroll);
      searchResponse = client.searchScroll(scrollRequest);
      scrollId = searchResponse.getScrollId();
      searchHits = searchResponse.getHits().getHits();

      System.out.println(searchHits.length + "hits from total hits: " + searchResponse.getHits().totalHits);

      total += searchHits.length;
    }

    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.addScrollId(scrollId);
    ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest);
    boolean succeeded = clearScrollResponse.isSucceeded();
    System.out.println("TOTAL: " + total);
    client.close();
  }

}

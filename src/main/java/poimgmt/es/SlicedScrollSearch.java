package poimgmt.es;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

/**
 * TODO {Purpose of This Class}
 *
 * @author MaibornWolff GmbH
 * @version 0.1
 * @since 25.10.18
 */
public class SlicedScrollSearch {

  int slices = 5;
  int scrollSize = 1000;
  TimeValue scrollTimeout = new TimeValue(60 * 1000);

  RestHighLevelClient client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200, "http")));

  public static void main(String[] args) {
    System.out.println("SlicedScrollSearch...");
    long start = System.currentTimeMillis();
    SlicedScrollSearch testClient = new SlicedScrollSearch();
    testClient.slicedScrollSearch();
    //testClient.test();
    long end = System.currentTimeMillis();
    System.out.println("SlicedScrollSearch-Time: " + (end - start));
    try {
      testClient.closeClient();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void closeClient() throws IOException {
    client.close();
  }

  private void slicedScrollSearch() {

    final int[] total = {0};

    final Scroll scroll;
    scroll = new Scroll(scrollTimeout);

    SearchRequest searchRequest = new SearchRequest("dcs");
    searchRequest.scroll(scroll);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    //searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchSourceBuilder
        .query(geoDistanceQuery("location").point(52.377498, 4.86902).distance(300, DistanceUnit.KILOMETERS));
    searchSourceBuilder.size(scrollSize);
    searchRequest.source(searchSourceBuilder);

    IntStream.range(0, slices).parallel().forEach(i -> {

      //prepare search
      SliceBuilder sliceBuilder = new SliceBuilder(i, slices);

      searchSourceBuilder.slice(sliceBuilder);

      // send the request and get the response here
      try {
        SearchResponse searchResponse = client.search(searchRequest);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        long totalHits = searchResponse.getHits().totalHits;
        //System.out.println("slice " + i + ", hits: " + searchHits.length + " from total hits: " + totalHits);
        total[0] += searchHits.length;
        System.out.println(searchHits.length);

        int count = 0;
        while (searchHits != null && searchHits.length > 0) {
          SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
          scrollRequest.scroll(scroll);

          searchResponse = client.searchScroll(scrollRequest);

          scrollId = searchResponse.getScrollId();
          searchHits = searchResponse.getHits().getHits();
          totalHits = searchResponse.getHits().totalHits;
          //System.out.println("slice " + i + ", hits: " + searchHits.length + " from total hits: " + totalHits);
          //System.out.println("Hits: " + searchHits.length);
          total[0] += searchHits.length;
          count ++;

          System.out.println(searchHits.length);
        }

        //System.out.println("COUNT: " + count);

      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    //System.out.println("TOTAL: " + total[0]);

  }

  private void test() {
    //search slice
    int slices = 5;
    int scrollSize = 10;
    TimeValue scrollTimeout = new TimeValue(60 * 1000);

    SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
    searchSourceBuilder.query(QueryBuilders.termQuery("name", "name"));

    IntStream.range(0, slices).parallel().forEach(i -> {
      //prepare search
      SliceBuilder sliceBuilder = new SliceBuilder(i, slices);

      final Scroll scroll;
      scroll = new Scroll(scrollTimeout);

      searchSourceBuilder
          .query(geoDistanceQuery("location").point(52.377498, 4.86902).distance(300, DistanceUnit.KILOMETERS));
      searchSourceBuilder.size(scrollSize);
      searchSourceBuilder.slice(sliceBuilder);

      SearchRequest searchRequest = new SearchRequest("dcs");
      searchRequest.scroll(scroll);
      searchRequest.source(searchSourceBuilder);

      SearchResponse response = null;
      try {
        response = client.search(searchRequest);
      } catch (IOException e) {
        e.printStackTrace();
      }

      List<String> r = Arrays.stream(response.getHits().getHits()).
          map(SearchHit::getSourceAsString).collect(Collectors.toList());
      System.out.println("slice " + i + " response: " + r);
    });
  }


}

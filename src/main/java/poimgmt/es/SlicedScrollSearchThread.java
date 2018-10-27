package poimgmt.es;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
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
public class SlicedScrollSearchThread {

  int slices = 5;
  int scrollSize = 1000;
  TimeValue scrollTimeout = new TimeValue(60 * 1000);

  RestHighLevelClient client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200, "http")));

  public static void main(String[] args) throws IOException {
    System.out.println("SlicedScrollSearch(BBoss)...");
    SlicedScrollSearchThread testClient = new SlicedScrollSearchThread();
    long start = System.currentTimeMillis();
    QueryBuilder queryBuilders = geoDistanceQuery("location").point(52.377498, 4.86902)
        .distance(300, DistanceUnit.KILOMETERS);
    testClient.searchSlicedScrolls(queryBuilders);
    long end = System.currentTimeMillis();
    System.out.println("SlicedScrollSearch-Time: " + (end - start));
    testClient.closeClient();
  }

  public void searchSlicedScrolls(QueryBuilder queryBuilders) {
    final int slicesMax = 5;
    ExecutorService singleThreadPool = Executors.newFixedThreadPool(slicesMax);

    final int[] total = {0};

    for (int id = 0; id < slicesMax; id++) {
      int finalId = id;
      singleThreadPool.submit(() -> {

        final Scroll scroll;
        scroll = new Scroll(scrollTimeout);

        SearchRequest searchRequest = new SearchRequest("dcs");
        searchRequest.scroll(scroll);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String[] excludeFields = new String[]{};

        String[] includeFields = new String[]{"dcs_cp_id", "latitude", "longitude", "dcs_pool_id"};
        searchSourceBuilder.fetchSource(includeFields, excludeFields);

        searchSourceBuilder
            .query(queryBuilders);
        searchSourceBuilder.size(scrollSize);
        searchRequest.source(searchSourceBuilder);

        SliceBuilder sliceBuilder = new SliceBuilder(finalId, slices);

        searchSourceBuilder.slice(sliceBuilder);

        SearchResponse searchResponse = null;
        try {
          searchResponse = client.search(searchRequest);
        } catch (IOException e) {
          e.printStackTrace();
        }

        SearchHit[] searchHits = searchResponse.getHits().getHits();

        total[0] += searchHits.length;

        // scroll
        if (searchHits != null && searchHits.length > 0) {

          // get the scroll id
          String scrollId = searchResponse.getScrollId();

          do {
            // get the next scroll
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);

            try {
              searchResponse = client.searchScroll(scrollRequest);
            } catch (IOException e) {
              e.printStackTrace();
            }

            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            total[0] += searchHits.length;
            if (searchHits != null && searchHits.length > 0) {
              // TODO retrieve and push result to the container
            } else {
              break;
            }
          } while (true);

        }
      });
    }
    singleThreadPool.shutdown();
    try {
      singleThreadPool.awaitTermination(100, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("Total: " + total[0]);
  }

  private void closeClient() throws IOException {
    client.close();
  }

}

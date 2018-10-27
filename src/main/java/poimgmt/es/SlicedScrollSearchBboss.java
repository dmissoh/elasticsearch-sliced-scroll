package poimgmt.es;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;

/**
 * TODO {Purpose of This Class}
 *
 * @author MaibornWolff GmbH
 * @version 0.1
 * @since 25.10.18
 */
public class SlicedScrollSearchBboss {

  public static void main(String[] args) {
    System.out.println("SlicedScrollSearch(BBoss)...");
    SlicedScrollSearchBboss testClient = new SlicedScrollSearchBboss();
    long start = System.currentTimeMillis();
    testClient.slicedScrollApiParallelHandler();
    long end = System.currentTimeMillis();
    System.out.println("SlicedScrollSearch-Time: " + (end - start));
  }

  public void slicedScrollApiParallelHandler() {
    ClientInterface clientUtil = ElasticSearchHelper.getConfigRestClientUtil("scroll.xml");

    //scroll slice
    int max = 15;

    Map params = new HashMap();
    params.put("sliceMax", max);
    params.put("size", 1000);

    ESDatas<Map> sliceResponse = clientUtil.scrollSlice("dcs/_search",
        "scrollSliceQuery", params, "1m", Map.class, response -> {
          List<Map> datas = response.getDatas();
          long totalSize = response.getTotalSize();
          //System.out.println("totalSize:" + totalSize + ",datas.size:" + datas.size());
        },
        true);

    long totalSize = sliceResponse.getTotalSize();
    System.out.println("totalSize:" + totalSize);
  }

}

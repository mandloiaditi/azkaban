package azkaban.project;

import azkaban.flow.Flow;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class CacheTest {

  final long shortTTL = 100L;
  final long longTTL = 10 * 10 * 100L;
  //  private Cache<String, Session> cache;
  private Project project;
  private Props props;

  @Before

  public void setUp() throws Exception {
    this.project = new Project(2, "test");
    this.props = new Props();
    final DirectoryFlowLoader loader = new DirectoryFlowLoader(this.props);
    loader.loadProjectFlow(this.project, ExecutionsTestUtil.getFlowDir("embedded"));
    Assert.assertEquals(0, loader.getErrors().size());
    this.project.setFlows(loader.getFlowMap());
    this.project.setVersion(123);
  }

  @Test
  public void testCache() throws Exception {
    final List<Flow> flows = this.project.getFlows();
    System.out.println(this.project.getName());
    final Cache<Integer, Project> cache = CacheBuilder.newBuilder()
        .maximumWeight(1100)
        .weigher((key, project) -> 100).build();
    for (int i = 0; i < 12; i++) {
      final Project project = new Project(i, "test");
      cache.put(i, project);
//      cache.getIfPresent(0);
//      System.out.println(cache.asMap().keySet());

    }
    System.out.println(cache.getIfPresent(0));
  }
}

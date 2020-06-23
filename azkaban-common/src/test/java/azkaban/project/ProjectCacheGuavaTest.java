package azkaban.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import azkaban.Constants.ConfigurationKeys;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.Props;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class ProjectCacheGuavaTest {

  private ProjectCacheGuava cacheGuava;
  private ProjectLoader projectLoader;
  private Props props;
  private CommonMetrics commonMetrics;

  @Before
  public void setUp() throws Exception {
    this.props = new Props();
    this.props.put(ConfigurationKeys.MAX_PROJECTS_CACHE, 3);
    this.commonMetrics = mock(CommonMetrics.class);
    this.projectLoader = mock(ProjectLoader.class);
    this.cacheGuava = new ProjectCacheGuava(this.props, this.projectLoader, this.commonMetrics);
  }

  // Common Metrics

  @Test
  public void testInitialization() {
    // Test with some recent project being loaded.
    this.props.put(ConfigurationKeys.INIT_NUM_PROJECTS, 1);
    when(this.projectLoader.fetchAllNames()).thenReturn(ImmutableMap.of("myTest1", 1, "myTest2", 2,
        "myTest3",
        3));
    when(this.projectLoader.fetchRecentProjects(1))
        .thenReturn(Arrays.asList(new Project(2, "myTest2")));
    this.cacheGuava = new ProjectCacheGuava(this.props, this.projectLoader, this.commonMetrics);
    assertEquals(this.cacheGuava.getProjectById(2).get(), this.cacheGuava.getProjectByName(
        "myTest2").get());

    // Testing when no results are fetched for recent projects.
    when(this.projectLoader.fetchRecentProjects(0))
        .thenReturn(Collections.emptyList());
    this.props.put(ConfigurationKeys.INIT_NUM_PROJECTS, 0);
    this.cacheGuava = new ProjectCacheGuava(this.props, this.projectLoader, this.commonMetrics);
    assert (!this.cacheGuava.getProjectById(2).isPresent());
    when(this.projectLoader.fetchProjectById(2)).thenReturn(new Project(2, "myTest2"));
    assert (this.cacheGuava.getProjectById(2).isPresent());
  }

  @Test
  public void testPutRemoveCache() {

    final Project test1 = new Project(1, "myProjectTest1");
    test1.setDescription("This is a project for testing.");
    final Project test2 = new Project(2, "myProjectTest2");
    test2.setDescription("This is another project for testing.");

    this.cacheGuava.putProject(test1);
    this.cacheGuava.putProject(test2);

    Optional<Project> ret = this.cacheGuava.getProjectById(1);
    assert (ret.isPresent());
    assertEquals(ret.get().getName(), test1.getName());
    assertEquals(ret.get().getDescription(), test1.getDescription());

    ret = this.cacheGuava.getProjectById(2);
    assertNotNull(ret);
    assertEquals(ret.get().getName(), test2.getName());
    assertEquals(ret.get().getId(), test2.getId());
    assertEquals(test2.getId(), 2);

    this.cacheGuava.removeProject(test1);

    ret = this.cacheGuava.getProjectById(test1.getId());
    assertNull(ret);
    ret = this.cacheGuava.getProjectByName(test1.getName());
    assertNull(ret);
  }

  @Test
  public void testProjectCacheHit() {
    when(this.projectLoader.fetchProjectById(1)).thenReturn(new Project(1, "test1"));
    final Project test1 = this.cacheGuava.getProjectById(1).orElseGet(null);
    final Project test2 = this.cacheGuava.getProjectById(1).orElse(null);
    final CacheStats stats = this.cacheGuava.getCacheStats();
    assertEquals(test1.getName(), test2.getName());
    assertEquals(stats.missCount(), 1);
    assertEquals(stats.hitCount(), 1);
  }

  @Test
  public void testEvictionPolicy() {

    final Project test1 = new Project(1, "myProjectTest1");
    final Project test2 = new Project(2, "myProjectTest2");
    final Project test3 = new Project(3, "myProjectTest3");
    this.cacheGuava.putProject(test1);
    this.cacheGuava.putProject(test2);
    this.cacheGuava.putProject(test3);
    this.cacheGuava.getProjectById(1);
    this.cacheGuava.getProjectById(3);
    this.cacheGuava.putProject(new Project(4, "myProjectTest4"));
    assertNull(this.cacheGuava.getProjectById(2).orElse(null));
    assertNotNull(this.cacheGuava.getProjectById(3));
    assertNotNull(this.cacheGuava.getProjectById(1));
    assertNotNull(this.cacheGuava.getProjectById(4));
  }

}

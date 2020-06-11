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
import java.util.List;
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
    assertEquals(this.cacheGuava.getAllProjectNames().size(), 3);
    assertEquals(this.cacheGuava.getProjectById(2), this.cacheGuava.getProjectByName("myTest2"));

    // Testing when no results are fetched for recent projects.
    when(this.projectLoader.fetchRecentProjects(0))
        .thenReturn(null);
    this.props.put(ConfigurationKeys.INIT_NUM_PROJECTS, 0);
    this.cacheGuava = new ProjectCacheGuava(this.props, this.projectLoader, this.commonMetrics);
    assertNull(this.cacheGuava.getProjectById(2));
    when(this.projectLoader.fetchProjectById(2)).thenReturn(new Project(2, "myTest2"));
    assertNotNull(this.cacheGuava.getProjectById(2));
  }

  @Test
  public void testPutRemoveCache() {

    final Project test1 = new Project(1, "myProjectTest1");
    test1.setDescription("This is a project for testing.");
    final Project test2 = new Project(2, "myProjectTest2");
    test2.setDescription("This is another project for testing.");

    this.cacheGuava.putProject(test1);
    this.cacheGuava.putProject(test2);

    Project ret = this.cacheGuava.getProjectById(1);
    assertNotNull(ret);
    assertEquals(ret.getName(), test1.getName());
    assertEquals(ret.getDescription(), test1.getDescription());

    ret = this.cacheGuava.getProjectById(2);
    final int id = this.cacheGuava.getProjectId(test2.getName());
    assertNotNull(ret);
    assertEquals(ret.getName(), test2.getName());
    assertEquals(ret.getId(), test2.getId());
    assertEquals(id, test2.getId());

    this.cacheGuava.removeProject(test1);

    ret = this.cacheGuava.getProjectById(test1.getId());
    assertNull(ret);
    ret = this.cacheGuava.getProjectByName(test1.getName());
    assertNull(ret);
    assertNull(this.cacheGuava.getProjectId(test1.getName()));
  }

  @Test
  public void testProjectCacheHit() {
    when(this.projectLoader.fetchProjectById(1)).thenReturn(new Project(1, "test1"));
    final Project test1 = this.cacheGuava.getProjectById(1);
    final Project test2 = this.cacheGuava.getProjectById(1);
    final CacheStats stats = this.cacheGuava.getCacheStats();
    assertEquals(test1.getName(), test2.getName());
    assertEquals(stats.missCount(), 1);
    assertEquals(stats.hitCount(), 1);
  }


  @Test
  public void testGetProjectNames() {
    final Project test = new Project(1, "myProjectTest1");
    test.setDescription("This is a project for testing.");
    this.cacheGuava.putProject(test);
    final List<String> names = this.cacheGuava.getAllProjectNames();
    assertNotNull(names);
    assertEquals(names.size(), 1);
    assertEquals(names.get(0), test.getName().toLowerCase());
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
    assertNull(this.cacheGuava.getProjectById(2));
    assertNotNull(this.cacheGuava.getProjectById(3));
    assertNotNull(this.cacheGuava.getProjectById(1));
    assertNotNull(this.cacheGuava.getProjectById(4));
  }

}

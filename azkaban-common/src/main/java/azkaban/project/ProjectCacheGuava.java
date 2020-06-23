/*
 * Copyright 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.project;

import azkaban.Constants.ConfigurationKeys;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.CaseInsensitiveConcurrentHashMap;
import azkaban.utils.Props;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements ProjectCache and extends AbstractProjectCache to implement a fixed size
 * guava cache implementation of project cache. Project "name to id mapping" for all the active
 * projects is loaded into the main-memory when the web-server starts. Also recently executed
 * projects are loaded into project cache, where project id is the key. The cache follows an LRU
 * policy and the maximum size and initial number of projects loaded are defined in configuration
 * keys.
 */
@Singleton
public class ProjectCacheGuava extends AbstractProjectCache implements ProjectCache {

  private static final int DEFAULT_INIT_NUM_PROJECTS = 100;
  private static final Logger logger = LoggerFactory.getLogger(ProjectCacheGuava.class);
  private static final int DEFAULT_MAX_NUM_PROJECTS = 1000;
  private final CaseInsensitiveConcurrentHashMap<Integer> nameToId;
  private final Cache<Integer, Project> cache;
  private final Props props;
  private final CommonMetrics commonMetrics;
  private final Random ran;

  @Inject
  public ProjectCacheGuava(final Props props, final ProjectLoader projectLoader,
      final CommonMetrics commonMetrics) {
    super(projectLoader);
    this.props = props;
    this.commonMetrics = commonMetrics;
    this.nameToId = new CaseInsensitiveConcurrentHashMap<>();
    this.cache = CacheBuilder.newBuilder()
        .maximumSize(this.props.getInt(ConfigurationKeys.MAX_PROJECTS_CACHE,
            DEFAULT_MAX_NUM_PROJECTS)).recordStats()
        .build();
    init();
    this.ran = new Random();
  }

  /**
   * load all active projects name to id mapping preloads cache with recently executed projects and
   * their corresponding flows into memory.
   */
  private void init() throws ProjectManagerException {
    /* initialize the name to id mapping */
    logger.info("Loading active projects names.");
    Map<String, Integer> allProjectNames = Collections.emptyMap();
    try {
      allProjectNames = fetchAllNames();
    } catch (final ProjectManagerException e) {
      throw new RuntimeException("Could not load projects from store.", e);
    }
    for (final String projName : allProjectNames.keySet()) {
      this.nameToId.put(projName, allProjectNames.get(projName));
    }

    // initialize the recently executed project entries in guava cache;
    logger.info("Loading active projects.");
    List<Project> result = Collections.emptyList();
    try {
      result = fetchRecentProjects(
          this.props.getInt(ConfigurationKeys.INIT_NUM_PROJECTS, DEFAULT_INIT_NUM_PROJECTS
          ));
    } catch (final ProjectManagerException e) {
      logger.info("Could not load cache.");
    }
    for (final Project proj : result) {
      this.cache.put(proj.getId(), proj);
    }
    loadAllFlows(result);
  }

  /**
   * Inserts given project into the cache.
   *
   * @param project Project
   */
  @Override
  public void putProject(final Project project) {
    this.nameToId.put(project.getName(), project.getId());
    this.cache.put(project.getId(), project);
  }

  /**
   * Fetches an active project by name.
   */
  @Override
  public Optional<Project> getProjectByName(final String key) {
    final Integer id = this.nameToId.get(key);
    if (id == null) {
      return Optional.empty();
    }
    return getProjectById(id);
  }

  // TODO  incorporate hit meter for comparison.
  // TODO  check if any methods need to be synchronised here.

  /**
   * Fetches any project active/inactive by project id. Any project
   */

  @Override
  public Optional<Project> getProjectById(final Integer id) throws ProjectManagerException {
    Project result = null;
    printCacheStats();
    try {
      result = this.cache.get(id,
          new Callable<Project>() {
            @Override
            public Project call() throws ProjectManagerException {
              ProjectCacheGuava.this.commonMetrics.markProjectCacheMiss();
              final Project res = fetchProjectById(id);
              final List<Project> project = Collections.singletonList(res);
              loadAllFlows(project);
              return res;
            }
          });
    } catch (final Exception e) {
      logger.info("Could not load project from store project id does not exist : " + id);
    }
    return Optional.ofNullable(result);
  }

  /**
   * Invalidates the given project from cache.
   */
  @Override
  public void removeProject(final Project project) {
    this.nameToId.remove(project.getName());
    this.cache.invalidate(project.getId());
  }

  /**
   * Returns projects with names matching to the given pattern. First it collects all ids from the
   * name to id mapping for the projects with the matching name. It then queries these ids
   * collectively from DB to fetch respective projects. It also updates the cache with these entries
   * as it is likely the searched projects will be accessed again.
   *
   * @param pattern
   * @return
   */
  @Override
  public List<Project> getProjectsWithSimilarNames(final Pattern pattern) {
    List<Project> matches = Collections.emptyList();
    final List<Integer> ids = new ArrayList<>();
    final ArrayList<String> names = new ArrayList<>(this.nameToId.getKeys());
    for (final String projectName : names) {
      if (pattern.matcher(projectName).find()) {
        ids.add(this.nameToId.get(projectName));
      }
    }
    try {
      matches = fetchProjectById(ids);
    } catch (final ProjectManagerException e) {
      logger.info("No matching projects found");
    }
    loadAllFlows(matches);
    for (final Project project : matches) {
      putProject(project);
    }
    return matches;
  }

  /**
   * Log prints the cache statistics 20 % of the time when cache is accessed.
   */
  public void printCacheStats() {
    final int print = this.ran.nextInt(99);
    if (print < 20) {
      logger.info(String.valueOf(this.cache.stats()));
      logger.info("Miss Rate for the Web Serve Project Cache :" + this.cache.stats().missRate());
    }
  }

  /**
   * returns guava cache stats for analysis
   */
  public CacheStats getCacheStats() {
    return this.cache.stats();
  }
}

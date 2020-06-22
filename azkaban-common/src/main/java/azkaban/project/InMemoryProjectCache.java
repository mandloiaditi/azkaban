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

import azkaban.utils.CaseInsensitiveConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements ProjectCache and extends AbstractProjectCache to implement a in-memory
 * implementation of project cache where all the active projects are loaded into the main-memory
 * when the web-server starts. This would be replaced in future by guava cache implementation.
 * <p>
 * The present cache consists of two mappings :  name to project  AND   id to project. In this
 * implementation both the maps contain all the project entities. In future implementations
 * name-to-project mapping will be replaced by name-to-id mapping containing all the active
 * projects' name-id and fixed size cache to store project entities.
 */
@Singleton
public class InMemoryProjectCache extends AbstractProjectCache implements ProjectCache {

  private static final Logger logger = LoggerFactory.getLogger(InMemoryProjectCache.class);

  private final ConcurrentHashMap<Integer, Project> projectsById;

  private final CaseInsensitiveConcurrentHashMap<Project> projectsByName;


  @Inject
  public InMemoryProjectCache(final ProjectLoader loader) {
    super(loader);
    this.projectsById = new ConcurrentHashMap<>();
    this.projectsByName = new CaseInsensitiveConcurrentHashMap<>();
    final long startTime = System.currentTimeMillis();
    init();
    final long elapsedTime = System.currentTimeMillis() - startTime;
    logger.info("Time taken to initialize and load cache in milliseconds: " + elapsedTime);
  }

  /**
   * load all active projects and their corresponding flows into memory. Queries from database only
   * returns a high level project object. Need to explicitly load flows for the project objects.
   */
  private void init() {
    final List<Project> projects = super.getActiveProjects();
    logger.info("Loading active projects.");
    if (projects != null && !projects.isEmpty()) {
      for (final Project proj : projects) {
        putProject(proj);
      }
      logger.info("Loading flows from active projects.");
      loadAllFlows(projects);
    }

  }

  /**
   * Inserts given project into the cache.
   *
   * @param project Project
   */
  @Override
  public void putProject(final Project project) {
    this.projectsByName.put(project.getName(), project);
    this.projectsById.put(project.getId(), project);
  }

  /**
   * Queries an active project by name. Fetches from database if not present in cache.
   *
   * @param key name of the project
   * @return Project
   */
  @Override
  public Project getProjectByName(final String key) {
    Project project = this.projectsByName.get(key);
    if (project == null) {
      logger.info("No active project with name {} exists in cache, fetching from DB.", key);
      try {
        project = fetchProjectByName(key);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return project;
  }

  /**
   * Fetch active/inactive project by project id. If active project not present in cache, fetches
   * from DB. Fetches inactive project from DB.
   *
   * @param key Project id
   * @return Project
   */
  @Override
  public Project getProjectById(final Integer key) {
    Project project = this.projectsById.get(key);
    if (project == null) {
      try {
        project = fetchProjectById(key);
      } catch (final ProjectManagerException e) {
        logger.error("Could not load project from store.", e);
      }
    }
    return project;
  }

  /**
   * Invalidates the given project from cache.
   */
  @Override
  public void removeProject(final Project project) {
    this.projectsByName.remove(project.getName());
    this.projectsById.remove(project.getId());
  }

  /**
   * Returns names for all the active projects.;
   */
  @Override
  public List<String> getAllProjectNames() {
    return this.projectsByName.getKeys();
  }

  /**
   * Returns id by querying the project name map.
   */
  @Override
  public Integer getProjectId(final String name) {
    return this.projectsByName.get(name).getId();
  }

  /**
   * Returns the projects corresponding to given list of ids from DB. We do not need to update
   * in-memory cache as it already has all of these projects with itself.
   */

  @Override
  public List<Project> fetchProjectForIds(final List<Integer> ids) throws ProjectManagerException {
    final ArrayList<Project> result = new ArrayList<>();
    if (!ids.isEmpty()) {
      for (final Integer id : ids) {
        result.add(getProjectById(id));
      }
    }
    if (result.isEmpty()) {
      throw new ProjectManagerException("No projects found for given ids");
    }
    return result;
  }

  /**
   * Returns all the projects from the in-memory cache map.
   */
  @Override
  public List<Project> getActiveProjects() {
    return new ArrayList<>(this.projectsById.values());
  }

}

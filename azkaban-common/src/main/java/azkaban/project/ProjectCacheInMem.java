package azkaban.project;

import azkaban.utils.CaseInsensitiveConcurrentHashMap;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class ProjectCacheInMem implements ProjectCache {

  private final ConcurrentHashMap<Integer, Project> projectsById =
      new ConcurrentHashMap<>();
  private final CaseInsensitiveConcurrentHashMap<Project> projectsByName =
      new CaseInsensitiveConcurrentHashMap<>();

  @Override
  public void putProject(final Project project) {
    this.projectsByName.put(project.getName(), project);
    this.projectsById.put(project.getId(), project);
  }

  @Override
  public Project getProjectByName(final String key) {
    return this.projectsByName.get(key);
  }

  @Override
  public Project getProjectById(final Integer key) {
    return this.projectsById.get(key);
  }

  @Override
  public void removeProject(final Project project) {
    this.projectsByName.remove(project.getName());
    this.projectsById.remove(project.getId());
  }

  @Override
  public Collection<Project> getAllProjects() {
    return this.projectsById.values();
  }
}

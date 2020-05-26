package azkaban.project;

import java.util.Collection;

public interface ProjectCache {

  void putProject(Project project);

  Project getProjectByName(final String key);

  Project getProjectById(final Integer id);

  void removeProject(Project project);

  public Collection<Project> getAllProjects();
}

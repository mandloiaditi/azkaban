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
 *
 */

package azkaban.project;

public enum ProjectCacheImplementationType {
  CACHE_ALL_INMEMORY(InMemoryProjectCache.class),
  CACHE_GUAVA(ProjectCacheGuava.class);

  private final Class<? extends AbstractProjectCache> implementationClass;

  ProjectCacheImplementationType(final Class<? extends AbstractProjectCache> implementationClass) {
    this.implementationClass = implementationClass;
  }

  public static azkaban.project.ProjectCacheImplementationType from(final String name) {
    try {
      return valueOf(name);
    } catch (final Exception e) {
      return CACHE_ALL_INMEMORY;
    }
  }

  public Class<? extends AbstractProjectCache> getImplementationClass() {
    return this.implementationClass;
  }
}

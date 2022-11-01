/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.testcontainers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

/** Interface for managing TestContainers resources in integration tests. */
public abstract class TestContainerResourceManager<T extends GenericContainer<?>> {
  private static final Logger LOG = LoggerFactory.getLogger(TestContainerResourceManager.class);

  private final T container;
  private final boolean usingStaticContainer;
  private final String host;
  private final int port;

  protected <B extends TestContainerResourceManager.Builder<?>> TestContainerResourceManager(
      T container, B builder) {
    this.container = container;
    this.usingStaticContainer = builder.useStaticContainer;
    this.host = builder.host;
    this.port = builder.port;

    if (!usingStaticContainer) {
      container.start();
    }
  }

  protected String getHost() {
    if (host == null) {
      return container.getHost();
    }
    return host;
  }

  protected int getPort(int mappedPort) {
    if (port < 0) {
      return container.getMappedPort(mappedPort);
    }
    return port;
  }

  /**
   * Deletes all created resources (VM's, etc.) and stops the container, making the manager object
   * unusable.
   *
   * @throws TestContainerResourceManagerException if there is an error deleting the TestContainers
   *     resources.
   */
  protected boolean cleanupAll() {
    LOG.info("Attempting to cleanup TestContainers manager.");

    if (!usingStaticContainer) {
      try {
        container.close();
      } catch (Exception e) {
        LOG.error("Failed to close TestContainer resources. {}", e.getMessage());
        return false;
      }
    } else {
      LOG.info(
          "This manager was configured to use a static resource container that will not be cleaned up.");
    }

    LOG.info("TestContainers manager successfully cleaned up.");
    return true;
  }

  /** Builder for {@link TestContainerResourceManager}. */
  public abstract static class Builder<T extends TestContainerResourceManager<?>> {

    public String testId;
    public String containerImageName;
    public String containerImageTag;
    public String host;
    public int port;
    public boolean useStaticContainer;

    public Builder(String testId) {
      this.testId = testId;
      this.port = -1;
    }

    /**
     * Sets the name of the test container image. The tag is typically the version of the image.
     *
     * @param containerName The name of the container image.
     * @return this builder object with the image name set.
     */
    public Builder<T> setContainerImageName(String containerName) {
      this.containerImageName = containerName;
      return this;
    }

    /**
     * Sets the tag for the test container. The tag is typically the version of the image.
     *
     * @param containerTag The tag to use for the container.
     * @return this builder object with the tag set.
     */
    public Builder<T> setContainerImageTag(String containerTag) {
      this.containerImageTag = containerTag;
      return this;
    }

    public Builder<T> setHost(String containerHost) {
      this.host = containerHost;
      return this;
    }

    public Builder<T> setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder<T> useStaticContainer() {
      this.useStaticContainer = true;
      return this;
    }

    public abstract T build();
  }
}

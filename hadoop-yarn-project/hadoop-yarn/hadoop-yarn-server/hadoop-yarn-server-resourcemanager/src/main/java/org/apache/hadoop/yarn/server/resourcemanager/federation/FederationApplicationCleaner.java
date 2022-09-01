/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class FederationApplicationCleaner implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationApplicationCleaner.class);
  private SubClusterId subClusterId;
  private FederationStateStore stateStoreService;
  private RMContext rmContext;

  public FederationApplicationCleaner(SubClusterId scId,
      FederationStateStore stateStoreClient, RMContext context) {
    this.subClusterId = scId;
    this.stateStoreService = stateStoreClient;
    this.rmContext = context;
    LOG.info("Initialized federation applicationcleaner for cluster with timestamp : {}.",
        ResourceManager.getClusterTimeStamp());
  }

  @Override
  public synchronized void run() {
    try {
      GetApplicationsHomeSubClusterRequest request =
          GetApplicationsHomeSubClusterRequest.newInstance();
      GetApplicationsHomeSubClusterResponse response =
          stateStoreService.getApplicationsHomeSubCluster(request);

      List<ApplicationHomeSubCluster> applicationHomeSubCluster =
          response.getAppsHomeSubClusters();
      // TODO: Filter SubCluster should be pushed down to the database
      List<ApplicationHomeSubCluster> applications = applicationHomeSubCluster.stream().filter(
          application -> application.getHomeSubCluster().equals(this.subClusterId)).
          collect(Collectors.toList());

      long successCleanUpAppCount = 0;
      for (ApplicationHomeSubCluster application : applications) {
        ApplicationId applicationId = application.getApplicationId();
        if (!this.rmContext.getRMApps().containsKey(applicationId)) {
          try {
            DeleteApplicationHomeSubClusterRequest deleteRequest =
                DeleteApplicationHomeSubClusterRequest.newInstance(applicationId);
            DeleteApplicationHomeSubClusterResponse deleteResponse =
                stateStoreService.deleteApplicationHomeSubCluster(deleteRequest);
            if (deleteResponse != null) {
              LOG.info("application = {} has been cleaned up successfully.", applicationId);
              successCleanUpAppCount++;
            }
          } catch (YarnException e) {
            LOG.error("problem during application = {} cleanup.", applicationId, e);
          }
        }
      }
      LOG.info("cleanup finished applications size = {}, number = {} successful cleanups.",
          applications.size(), successCleanUpAppCount);
    } catch (Exception e) {
      LOG.error("problem during cleanup applications.", e);
    }
  }
}

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
package org.apache.hadoop.yarn.server.router.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TestSubClusterCleaner {

  ////////////////////////////////
  // Router Constants
  ////////////////////////////////
  private Configuration conf;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreFacade facade;
  private SubClusterCleaner cleaner;

  @Before
  public void setup() throws YarnException {
    conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.ROUTER_SUBCLUSTER_EXPIRATION_TIME, 1000);
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);

    facade = FederationStateStoreFacade.getInstance();
    facade.reinitialize(stateStore, conf);

    cleaner = new SubClusterCleaner(conf);
    for (int i = 0; i < 4; i++){
      // Create sub cluster id and info
      SubClusterId subClusterId = SubClusterId.newInstance("SC-" + i);
      SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
          "127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3", "127.0.0.1:4",
           SubClusterState.SC_RUNNING, System.currentTimeMillis(), "");
      // Register the subCluster
      stateStore.registerSubCluster(
          SubClusterRegisterRequest.newInstance(subClusterInfo));
    }
  }

  @Test
  public void testSubClusterRegisterHeartBeatTime() throws YarnException, InterruptedException {
    Thread.sleep(1000);
    cleaner.run();
    Map<SubClusterId, SubClusterInfo> activeSubClusterMapping = facade.getSubClusters(true);
    Assert.assertEquals(0, activeSubClusterMapping.size());
  }
}

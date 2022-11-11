/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for ZookeeperFederationStateStore.
 */
public class TestZookeeperFederationStateStore
    extends FederationStateStoreBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestZookeeperFederationStateStore.class);

  /** Zookeeper test server. */
  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;

  @Before
  public void before() throws IOException, YarnException {
    try {
      curatorTestingServer = new TestingServer();
      curatorTestingServer.start();
      String connectString = curatorTestingServer.getConnectString();
      curatorFramework = CuratorFrameworkFactory.builder()
          .connectString(connectString)
          .retryPolicy(new RetryNTimes(100, 100))
          .build();
      curatorFramework.start();

      Configuration conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);
      conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS, 10);
      setConf(conf);
    } catch (Exception e) {
      LOG.error("Cannot initialize ZooKeeper store", e);
      throw new IOException(e);
    }

    super.before();
  }

  @After
  public void after() throws Exception {
    super.after();
    curatorFramework.close();
    try {
      curatorTestingServer.stop();
    } catch (IOException e) {
    }
  }

  @Override
  protected FederationStateStore createStateStore() {
    super.setConf(getConf());
    return new ZookeeperFederationStateStore();
  }

  @Test
  public void testMetricsInited() throws Exception {
    ZookeeperFederationStateStore zkStateStore = (ZookeeperFederationStateStore) createStateStore();
    ZKFederationStateStoreOpDurations zkStateStoreOpDurations = zkStateStore.getOpDurations();
    MetricsCollectorImpl collector = new MetricsCollectorImpl();

    long anyDuration = 10;
    long start = Time.now();
    long end = start + anyDuration;

    zkStateStoreOpDurations.addAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addUpdateAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetAppsHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addDeleteAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addRegisterSubClusterDuration(start, end);
    zkStateStoreOpDurations.addDeregisterSubClusterDuration(start, end);
    zkStateStoreOpDurations.addSubClusterHeartbeatDuration(start, end);
    zkStateStoreOpDurations.addGetSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetSubClustersDuration(start, end);
    zkStateStoreOpDurations.addGetPolicyConfigurationDuration(start, end);
    zkStateStoreOpDurations.addSetPolicyConfigurationDuration(start, end);
    zkStateStoreOpDurations.addGetPoliciesConfigurationsDuration(start, end);
    zkStateStoreOpDurations.addReservationHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetReservationHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetReservationsHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addDeleteReservationHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addUpdateReservationHomeSubClusterDuration(start, end);

    zkStateStoreOpDurations.getMetrics(collector, true);
    assertEquals("Incorrect number of perf metrics", 1, collector.getRecords().size());

    MetricsRecord record = collector.getRecords().get(0);
    MetricsRecords.assertTag(record, ZKFederationStateStoreOpDurations.RECORD_INFO.name(),
        "ZKFederationStateStoreOpDurations");

    double expectAvgTime = anyDuration;
    MetricsRecords.assertMetric(record, "AddAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "UpdateAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetAppsHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeleteAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "RegisterSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeregisterSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "SubClusterHeartbeatAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetSubClustersAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetPolicyConfigurationAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "SetPolicyConfigurationAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetPoliciesConfigurationsAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "AddReservationHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetReservationHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetReservationsHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeleteReservationHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "UpdateReservationHomeSubClusterAvgTime",  expectAvgTime);

    long expectOps = 1;
    MetricsRecords.assertMetric(record, "AddAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "UpdateAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetAppsHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeleteAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "RegisterSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeregisterSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "SubClusterHeartbeatNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetSubClustersNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetPolicyConfigurationNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "SetPolicyConfigurationNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetPoliciesConfigurationsNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "AddReservationHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetReservationHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetReservationsHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeleteReservationHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "UpdateReservationHomeSubClusterNumOps",  expectOps);
  }

  public void testStoreNewMasterKey() throws Exception {
    // We Will Design a unit test like this:
    // 1. Manually create a DelegationKey,
    // and call the interface storeNewMasterKey to write the data to zk.
    // 2. Compare the data returned by the storeNewMasterKey
    // interface with the data queried by zk, and ensure that the data is consistent.
    DelegationKey key = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    RouterMasterKey paramRouterMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    FederationStateStore stateStore = this.getStateStore();

    assertTrue(stateStore instanceof ZookeeperFederationStateStore);

    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(paramRouterMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    assertNotNull(response);
    RouterMasterKey respRouterMasterKey = response.getRouterMasterKey();
    assertNotNull(respRouterMasterKey);

    // Get Data From ZK.
    String nodeName = "delegation_key_" + key.getKeyId();
    String nodePath = "/federationstore/router_rm_dt_secret_manager_root/" +
        "router_rm_dt_master_keys_root/" + nodeName;
    byte[] data = curatorFramework.getData().forPath(nodePath);
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    DataInputStream din = new DataInputStream(bin);
    DelegationKey zkDT = new DelegationKey();
    zkDT.readFields(din);

    // Generate RouterMasterKey based on ZK data.
    RouterMasterKey zkRouterMasterKey =
        RouterMasterKey.newInstance(zkDT.getKeyId(), ByteBuffer.wrap(zkDT.getEncodedKey()),
        zkDT.getExpiryDate());
    assertNotNull(zkRouterMasterKey);
    assertEquals(paramRouterMasterKey, respRouterMasterKey);
    assertEquals(paramRouterMasterKey, zkRouterMasterKey);
    assertEquals(zkRouterMasterKey, respRouterMasterKey);
  }

  public void testGetMasterKeyByDelegationKey() throws Exception {
    // We will design a unit test like this:
    // 1. Manually create a DelegationKey,
    // and call the interface storeNewMasterKey to write the data to zk.
    // 2. Call the getMasterKeyByDelegationKey interface of stateStore to get the MasterKey data.
    // 3. The zk data should be consistent with the returned data.
    DelegationKey key = new DelegationKey(5678, 8765, "keyBytes".getBytes());
    RouterMasterKey paramRouterMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    FederationStateStore stateStore = this.getStateStore();

    assertTrue(stateStore instanceof ZookeeperFederationStateStore);

    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(paramRouterMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    assertNotNull(response);

    // Get Data From ZK.
    String nodeName = "delegation_key_" + key.getKeyId();
    String nodePath = "/federationstore/router_rm_dt_secret_manager_root/" +
        "router_rm_dt_master_keys_root/" + nodeName;
    byte[] data = curatorFramework.getData().forPath(nodePath);
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    DataInputStream din = new DataInputStream(bin);
    DelegationKey zkDT = new DelegationKey();
    zkDT.readFields(din);
    RouterMasterKey zkRouterMasterKey =
        RouterMasterKey.newInstance(zkDT.getKeyId(), ByteBuffer.wrap(zkDT.getEncodedKey()),
        zkDT.getExpiryDate());

    // Call the getMasterKeyByDelegationKey interface to get the returned result.
    RouterMasterKeyResponse response1 = stateStore.getMasterKeyByDelegationKey(routerMasterKeyRequest);
    assertNotNull(response1);
    RouterMasterKey respRouterMasterKey = response1.getRouterMasterKey();
    assertEquals(paramRouterMasterKey, respRouterMasterKey);
    assertEquals(paramRouterMasterKey, zkRouterMasterKey);
    assertEquals(zkRouterMasterKey, respRouterMasterKey);
  }

  public void testRemoveStoredMasterKey() throws Exception {

    // We will design a unit test like this:
    // 1. Manually create a DelegationKey,
    // and call the interface storeNewMasterKey to write the data to zk.
    // 2. Call removeStoredMasterKey to remove the MasterKey data in zk.
    // 3. Check if Zk data exists, data should not exist.

    // Step1. Manually create a DelegationKey，store Data in zk.
    DelegationKey key = new DelegationKey(2345, 5432, "keyBytes".getBytes());
    RouterMasterKey paramRouterMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    FederationStateStore stateStore = this.getStateStore();

    assertTrue(stateStore instanceof ZookeeperFederationStateStore);

    // We need to ensure that the returned result is not empty.
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(paramRouterMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    assertNotNull(response);

    // We will check if delegationToken exists in zk.
    String nodeName = "delegation_key_" + key.getKeyId();
    String nodePath = "/federationstore/router_rm_dt_secret_manager_root/" +
        "router_rm_dt_master_keys_root/" + nodeName;
    assertTrue(curatorFramework.checkExists().forPath(nodePath) != null);

    // Step2. Call removeStoredMasterKey
    RouterMasterKeyResponse response1 = stateStore.removeStoredMasterKey(routerMasterKeyRequest);
    assertNotNull(response1);
    RouterMasterKey respRouterMasterKey = response1.getRouterMasterKey();
    assertNotNull(respRouterMasterKey);
    assertEquals(paramRouterMasterKey, respRouterMasterKey);

    // We have removed the RouterMasterKey data from zk,
    // the path should be empty at this point.
    assertTrue(curatorFramework.checkExists().forPath(nodePath) == null);
  }

  public void testStoreNewToken() throws IOException, YarnException {
    super.testStoreNewToken();
  }

  public void testUpdateStoredToken() throws IOException, YarnException {
    super.testUpdateStoredToken();
  }

  public void testRemoveStoredToken() throws IOException, YarnException {
    super.testRemoveStoredToken();
  }

  public void testGetTokenByRouterStoreToken() throws IOException, YarnException {
    super.testGetTokenByRouterStoreToken();
  }
}
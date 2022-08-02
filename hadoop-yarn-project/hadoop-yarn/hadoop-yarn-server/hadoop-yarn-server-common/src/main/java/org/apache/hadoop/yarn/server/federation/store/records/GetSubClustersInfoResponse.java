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

package org.apache.hadoop.yarn.server.federation.store.records;

import java.util.List;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * Response to a query with list of {@link SubClusterInfo} about all
 * sub-clusters that are currently participating in Federation.
 */
@Private
@Unstable
public abstract class GetSubClustersInfoResponse {

  @Public
  @Unstable
  public static GetSubClustersInfoResponse newInstance(
      List<SubClusterInfo> subClusters) {
    GetSubClustersInfoResponse subClusterInfos =
        Records.newRecord(GetSubClustersInfoResponse.class);
    subClusterInfos.setSubClusters(subClusters);
    return subClusterInfos;
  }

  @Public
  @Unstable
  public static GetSubClustersInfoResponse newInstance(
      Collection<SubClusterInfo> subClusters) {
    GetSubClustersInfoResponse subClusterInfos =
            Records.newRecord(GetSubClustersInfoResponse.class);
    subClusterInfos.setSubClusters(subClusters);
    return subClusterInfos;
  }

  /**
   * Get the list of {@link SubClusterInfo} representing the information about
   * all sub-clusters that are currently participating in Federation.
   *
   * @return the list of {@link SubClusterInfo}
   */
  @Public
  @Unstable
  public abstract List<SubClusterInfo> getSubClusters();

  /**
   * Set the list of {@link SubClusterInfo} representing the information about
   * all sub-clusters that are currently participating in Federation.
   *
   * @param subClusters the list of {@link SubClusterInfo}
   */
  @Private
  @Unstable
  public abstract void setSubClusters(List<SubClusterInfo> subClusters);

  /**
   * Set the Collection of {@link SubClusterInfo} representing the information about
   * all sub-clusters that are currently participating in Federation.
   *
   * @param subClusters the list of {@link SubClusterInfo}
   */
  @Private
  @Unstable
  public abstract void setSubClusters(Collection<SubClusterInfo> subClusters);
}

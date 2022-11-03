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

package org.apache.hadoop.yarn.server.router.webapp;

import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Navigation block for the Router Web UI.
 */
public class NavBlock extends RouterBlock {

  private Router router;

  @Inject
  public NavBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
    initTestFederationSubCluster();
  }

  @Override
  public void render(Block html) {
    Hamlet.UL<Hamlet.DIV<Hamlet>> mainList = html.div("#nav").
        h3("Cluster").
        ul().
        li().a(url(""), "About").__().
        li().a(url("federation"), "Federation").__();

    List<String> subClusterIds = getActiveSubClusterIds();

    // ### nodes info
    initNodesMenu(mainList, subClusterIds);

    // ### nodelabels info
    initNodeLabelsMenu(mainList, subClusterIds);

    // ### applications info
    initApplicationsMenu(mainList, subClusterIds);

    // ### tools
    Hamlet.DIV<Hamlet> sectionBefore = mainList.__();
    Configuration conf = new Configuration();
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(sectionBefore, conf);

    if (tools == null) {
      return;
    }

    tools.__().__();
  }

  public void initTestFederationSubCluster() {
    try {
      // Initialize subcluster information
      String scAmRMAddress = "5.6.7.8:5";
      String scClientRMAddress = "5.6.7.8:6";
      String scRmAdminAddress = "5.6.7.8:7";
      String scWebAppAddress = "127.0.0.1:8080";

      // Initialize subcluster capability
      String[] capabilityPathItems = new String[]{".", "target", "test-classes", "capability"};
      String capabilityPath = StringUtils.join(capabilityPathItems, File.separator);
      String capabilityJson =
              FileUtils.readFileToString(new File(capabilityPath), StandardCharsets.UTF_8);

      // capability json needs to remove asflicense
      String regex = "\"___asflicense__.*\\n(.*,\\n){1,15}.*\\n.*";
      Pattern p = Pattern.compile(regex);
      Matcher m = p.matcher(capabilityJson);
      capabilityJson = m.replaceAll("").trim();

      // Initialize subcluster sc1
      SubClusterInfo sc1 =
              SubClusterInfo.newInstance(SubClusterId.newInstance("SC-1"),
                      scAmRMAddress, scClientRMAddress, scRmAdminAddress, scWebAppAddress,
                      SubClusterState.SC_RUNNING, Time.now(), capabilityJson);
      sc1.setLastHeartBeat(Time.now());

      FederationStateStore stateStore = getFacade().getStateStore();
      stateStore.registerSubCluster(SubClusterRegisterRequest.newInstance(sc1));
    }catch (Exception e){

    }
  }
}

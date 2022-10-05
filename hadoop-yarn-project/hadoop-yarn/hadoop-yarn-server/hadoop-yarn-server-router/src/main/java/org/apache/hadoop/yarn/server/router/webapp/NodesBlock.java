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

import com.sun.jersey.api.client.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * Nodes block for the Router Web UI.
 */
public class NodesBlock extends HtmlBlock {

  private static final long BYTES_IN_MB = 1024 * 1024;

  private final Router router;

  @Inject
  NodesBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {

    Hamlet.DIV<Hamlet> div = html.div().$class("alert alert-dismissable alert-info");
    div.p().$style("color:red").__("Federation is not Enabled.").__()
            .p().__()
            .p().__("We can refer to the following documents to configure Yarn Federation. ").__()
            .p().__()
            .a("https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/Federation.html",
                    "Hadoop: YARN Federation").
            __();

    html.__(MetricsOverviewTable.class);


    // Get the node info from the federation
    Configuration conf = this.router.getConfig();
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
    NodesInfo nodes = RouterWebServiceUtil
        .genericForward(webAppAddress, null, NodesInfo.class, HTTPMethods.GET,
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES, null, null, conf,
            client);

    setTitle("Nodes");

    TBODY<TABLE<Hamlet>> tbody = html.table("#nodes").thead().tr()
        .th(".nodelabels", "Node Labels")
        .th(".rack", "Rack")
        .th(".state", "Node State")
        .th(".nodeaddress", "Node Address")
        .th(".nodehttpaddress", "Node HTTP Address")
        .th(".lastHealthUpdate", "Last health-update")
        .th(".healthReport", "Health-report")
        .th(".containers", "Containers")
        .th(".mem", "Mem Used")
        .th(".mem", "Mem Avail")
        .th(".vcores", "VCores Used")
        .th(".vcores", "VCores Avail")
        .th(".nodeManagerVersion", "Version")
        .__().__().tbody();

    // Add nodes to the web UI
    for (NodeInfo info : nodes.getNodes()) {
      int usedMemory = (int) info.getUsedMemory();
      int availableMemory = (int) info.getAvailableMemory();
      TR<TBODY<TABLE<Hamlet>>> row = tbody.tr();
      row.td().__("N/A").__();
      row.td().__("N/A").__();
      row.td().__("N/A").__();
      row.td().__("N/A").__();
      boolean isInactive = false;
      if (isInactive) {
        row.td().__("N/A").__();
      } else {
        String httpAddress = info.getNodeHTTPAddress();
        row.td().a("//" + httpAddress, httpAddress).__();
      }
      row.td().br().$title(String.valueOf(info.getLastHealthUpdate())).__()
          .__("N/A").__()
          .td("N/A")
          .td("N/A").td().br()
          .$title(String.valueOf(usedMemory)).__()
          .__("N/A").__().td().br()
          .$title(String.valueOf(availableMemory)).__()
          .__("N/A").__()
          .td("N/A")
          .td("N/A")
          .td("N/A").__();
    }
    tbody.__().__();
  }
}

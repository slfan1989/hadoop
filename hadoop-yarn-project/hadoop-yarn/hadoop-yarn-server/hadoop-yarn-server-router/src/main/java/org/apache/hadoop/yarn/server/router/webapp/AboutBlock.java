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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * About block for the Router Web UI.
 */
public class AboutBlock extends HtmlBlock {

  private static final long BYTES_IN_MB = 1024 * 1024;

  private final Router router;

  @Inject
  AboutBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    Hamlet.DIV<Hamlet> div = html.div().$class("alert alert-dismissable alert-info");
    div.p().__("Federation is not Enabled.").__()
            .p().__()
            .p().__("We can refer to the following documents to configure Yarn Federation. ").__()
            .p().__()
            .a("https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/Federation.html",
                    "Hadoop: YARN Federation").
            __();

    html.__(MetricsOverviewTable.class);

    Configuration conf = this.router.getConfig();
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
    Client client = RouterWebServiceUtil.createJerseyClient(conf);

    ClusterMetricsInfo metrics = RouterWebServiceUtil
        .genericForward(webAppAddress, null, ClusterMetricsInfo.class,
            HTTPMethods.GET,
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
            conf, client);
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    info("Yarn Router Overview").
            __("Federation Enabled:", "true").
            __("Router ID:", System.currentTimeMillis()).
            __("Router state:", "RUNNING").
            __("Router SubCluster Count:", 4).
            __("Router RMStateStore:",
            "org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore").
            __("Router started on:", "星期日 九月 25 11:07:18 +0800 2022").
            __("Router version:", "3.4.0-SNAPSHOT from 128f8cdebd34c10d3267e018b15a98d2e5612a57 by fanshilun source checksum d31deaf4cff0b954cc855f455e3b8ef on 2022-09-24T02:08Z").
            __("Hadoop version:", "3.4.0-SNAPSHOT from 128f8cdebd34c10d3267e018b15a98d2e5612a57 by fanshilun source checksum d31deaf4cff0b954cc855f455e3b8ef on 2022-09-24T02:08Z");




    html.__(InfoBlock.class);
  }
}
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
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterInfo;
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

    Configuration conf = this.router.getConfig();
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
    Client client = RouterWebServiceUtil.createJerseyClient(conf);

    ClusterMetricsInfo metrics = RouterWebServiceUtil
        .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
        conf, client);

    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);


    Hamlet.DIV<Hamlet> div = html.div().$class("alert alert-dismissable alert-info");
    div.p().__("Federation is not Enabled.").__()
            .p().__()
            .p().__("We can refer to the following documents to configure Yarn Federation. ").__()
            .p().__()
            .a("https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/Federation.html",
                    "Hadoop: YARN Federation").
            __();

    html.__(MetricsOverviewTable.class);

    RouterInfo routerInfo = new RouterInfo(router);
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance();

    String lastStartTime =
            DateFormatUtils.format(routerInfo.getStartedOn(), DATE_PATTERN);

    try {
      info("Yarn Router Overview").
              __("Federation Enabled:", String.valueOf(isEnabled)).
              __("Router ID:", routerInfo.getClusterId()).
              __("Router state:", routerInfo.getState()).
              __("Router SubCluster Count:", facade.getSubClusters(false).size()).
              __("Router RMStateStore:", routerInfo.getRouterStateStore()).
              __("Router started on:", lastStartTime).
              __("Router version:", routerInfo.getRouterBuildVersion() +
                  " on " + routerInfo.getRouterVersionBuiltOn()).
              __("Hadoop version:", routerInfo.getHadoopBuildVersion() +
                  " on " + routerInfo.getHadoopVersionBuiltOn());
    } catch (YarnException e) {
      e.printStackTrace();
    }

    html.__(InfoBlock.class);
  }
}
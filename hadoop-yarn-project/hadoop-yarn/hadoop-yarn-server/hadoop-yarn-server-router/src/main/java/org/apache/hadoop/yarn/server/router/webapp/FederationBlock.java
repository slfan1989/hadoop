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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;
import org.terracotta.statistics.Time;

class FederationBlock extends HtmlBlock {

  private static final long BYTES_IN_MB = 1024 * 1024;

  private final Router router;

  @Inject
  FederationBlock(ViewContext ctx, Router router) {
    super(ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {
    html.script().$type("text/javascript").
            __("$(document).ready(function() {" +
                    " var table = $('#rms').DataTable();" +
                    " $('#rms tbody').on('click', 'td.details-control', function () {" +
                    " var tr = $(this).closest('tr');  " +
                    " var row = table.row( tr ); " +
                    " row.child('<table><tr><td>123</td><td>456</td><td>789</td></tr></table>').show(); });  });").__();
    Configuration conf = this.router.getConfig();
    boolean isEnabled = conf.getBoolean(
            YarnConfiguration.FEDERATION_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    if (!isEnabled) {

      // Table header
      TBODY<TABLE<Hamlet>> tbody = html.table("#rms").thead().tr()
              .th(".id", "SubCluster")
              .th(".state", "SubCluster State")
              .th(".lastStartTime", "LastStartTime")
              .th(".lastHeartBeat", "LastHeartBeat")
              .th(".failedA", "Applications Failed*")
              .th(".killedA", "Applications Killed*")
              .__().__().tbody();

      try {
        // Binding to the FederationStateStore
        FederationStateStoreFacade facade =
                FederationStateStoreFacade.getInstance();

        String sc1AmRMAddress = "5.6.7.8:5";
        String sc1ClientRMAddress = "5.6.7.8:6";
        String sc1RmAdminAddress = "5.6.7.8:7";
        String sc1WebAppAddress = "0.0.0.0:8080";


        String json = FileUtils.readFileToString(new File("/Users/fanshilun/Documents/code-v3/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-router/target/json"));
        SubClusterInfo sc1 =
            SubClusterInfo.newInstance(SubClusterId.newInstance("SC-1"),
            sc1AmRMAddress, sc1ClientRMAddress, sc1RmAdminAddress, sc1WebAppAddress,
            SubClusterState.SC_RUNNING, new Date().getTime(), json);
        Thread.sleep(1000);
        sc1.setLastHeartBeat(new Date().getTime());

        facade.getStateStore().registerSubCluster(SubClusterRegisterRequest.newInstance(sc1));

        Map<SubClusterId, SubClusterInfo> subClustersInfo =
                facade.getSubClusters(true);

        // Sort the SubClusters
        List<SubClusterInfo> subclusters = new ArrayList<>();
        subclusters.addAll(subClustersInfo.values());
        Comparator<? super SubClusterInfo> cmp =
                new Comparator<SubClusterInfo>() {
                  @Override
                  public int compare(SubClusterInfo o1, SubClusterInfo o2) {
                    return o1.getSubClusterId().compareTo(o2.getSubClusterId());
                  }
                };
        Collections.sort(subclusters, cmp);

        for (SubClusterInfo subcluster : subclusters) {
          SubClusterId subClusterId = subcluster.getSubClusterId();
          String webAppAddress = subcluster.getRMWebServiceAddress();
          String capability = subcluster.getCapability();
          ClusterMetricsInfo subClusterInfo = getClusterMetricsInfo(capability);



          // Building row per SubCluster
          // $class("details-control")
          // .$onclick("tdclick()")
          tbody.tr().td().$class("details-control").__(subClusterId.toString()).__()
                  .td(subcluster.getState().name())
                  .td(DateFormatUtils.format(subcluster.getLastStartTime(),"yyyy-MM-dd HH:mm:ss"))
                  .td(DateFormatUtils.format(subcluster.getLastHeartBeat(),"yyyy-MM-dd HH:mm:ss"))
                  .td(Integer.toString(subClusterInfo.getAppsFailed()))
                  .td(Integer.toString(subClusterInfo.getAppsKilled()))
          .__();
        }
      } catch (YarnException | IOException | InterruptedException e) {
        LOG.error("Cannot render ResourceManager", e);
      }

      tbody.__().__().div()
              .p().__("*The application counts are local per subcluster").__().__();

    } else {
      Hamlet.DIV<Hamlet> div = html.div("#div_id");
      div.p().__("Federation is not Enabled.").__().p().__("We can refer to the following documents to configure Yarn Federation. ").__()
         .a("https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/Federation.html","[Hadoop: YARN Federation]").__();
    }
  }

  private static ClusterMetricsInfo getClusterMetricsInfo(String capability) {
    ClusterMetricsInfo clusterMetrics = null;
    try {
      JSONJAXBContext jc = new JSONJAXBContext(
              JSONConfiguration.mapped().rootUnwrapping(false).build(),
              ClusterMetricsInfo.class);
      JSONUnmarshaller unmarshaller = jc.createJSONUnmarshaller();
      clusterMetrics = unmarshaller.unmarshalFromJSON(
              new StringReader(capability), ClusterMetricsInfo.class);
    } catch (Exception e) {
      LOG.error("Cannot parse SubCluster info", e);
    }
    return clusterMetrics;
  }
}

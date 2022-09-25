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
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Arrays;

/**
 * Provides an table with an overview of many cluster wide metrics and if
 * per user metrics are enabled it will show an overview of what the
 * current user is using on the cluster.
 */
public class MetricsOverviewTable extends HtmlBlock {
  private static final long BYTES_IN_MB = 1024 * 1024;

  private final ResourceManager rm;

  @Inject
  MetricsOverviewTable(ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.rm = rm;
  }

  @Override
  protected void render(Block html) {
    // Yes this is a hack, but there is no other way to insert
    // CSS in the correct spot
    html.style(".metrics {margin-bottom:5px}");
    html.style(".alert {padding: 15px; margin-bottom: 20px; border: 1px solid transparent; border-radius: 4px;}");
    html.style(".alert-dismissable {padding-right: 35px;}");
    html.style(".alert-info {color: #856404;background-color: #fff3cd;border-color: #ffeeba;}");

    DIV<Hamlet> div = html.div().$class("metrics");

    div.h3("Federation Cluster Metrics").
            table("#metricsoverview").
            thead().$class("ui-widget-header").
            tr().
            th().$class("ui-state-default").__("Apps Submitted").__().
            th().$class("ui-state-default").__("Apps Pending").__().
            th().$class("ui-state-default").__("Apps Running").__().
            th().$class("ui-state-default").__("Apps Completed").__().
            th().$class("ui-state-default").__("Containers Running").__().
            th().$class("ui-state-default").__("Used Resources").__().
            th().$class("ui-state-default").__("Total Resources").__().
            th().$class("ui-state-default").__("Reserved Resources").__().
            th().$class("ui-state-default").__("Physical Mem Used %").__().
            th().$class("ui-state-default").__("Physical VCores Used %").__().
            __().
            __().
            tbody().$class("ui-widget-content").
            tr().
            td(String.valueOf(1)).
            td(String.valueOf(1)).
            td(String.valueOf(1)).
            td(
                    String.valueOf(
                            3
                    )
            ).
            td(String.valueOf(123)).
            td("<memory:0 B, vCores:0>").
            td("<memory:0 B, vCores:0>").
            td("<memory:0 B, vCores:0>").
            td(String.valueOf("10%")).
            td(String.valueOf("10%")).
            __().
            __().__();

    div.h3("Federation Cluster Nodes Metrics").
            table("#nodemetricsoverview").
            thead().$class("ui-widget-header").
            tr().
            th().$class("ui-state-default").__("Active Nodes").__().
            th().$class("ui-state-default").__("Decommissioning Nodes").__().
            th().$class("ui-state-default").__("Decommissioned Nodes").__().
            th().$class("ui-state-default").__("Lost Nodes").__().
            th().$class("ui-state-default").__("Unhealthy Nodes").__().
            th().$class("ui-state-default").__("Rebooted Nodes").__().
            th().$class("ui-state-default").__("Shutdown Nodes").__().
            __().
            __().
            tbody().$class("ui-widget-content").
            tr().
            td().a(url("nodes"), String.valueOf(10)).__().
            td().a(url("nodes/decommissioning"), String.valueOf(2)).__().
            td().a(url("nodes/decommissioned"), String.valueOf(3)).__().
            td().a(url("nodes/lost"), String.valueOf(2)).__().
            td().a(url("nodes/unhealthy"), String.valueOf(1)).__().
            td().a(url("nodes/rebooted"), String.valueOf(1)).__().
            td().a(url("nodes/shutdown"), String.valueOf(1)).__().
            __().
            __().__();


    div.h3("Federation Cluster Scheduler Metrics").
            table("#schedulermetricsoverview").
            thead().$class("ui-widget-header").
            tr().
            th().$class("ui-state-default").__("SubCluster").__().
            th().$class("ui-state-default").__("Scheduler Type").__().
            th().$class("ui-state-default").__("Scheduling Resource Type").__().
            th().$class("ui-state-default").__("Minimum Allocation").__().
            th().$class("ui-state-default").__("Maximum Allocation").__().
            th().$class("ui-state-default")
            .__("Maximum Cluster Application Priority").__().
            th().$class("ui-state-default").__("Scheduler Busy %").__().
            th().$class("ui-state-default")
            .__("RM Dispatcher EventQueue Size").__().
            th().$class("ui-state-default")
            .__("Scheduler Dispatcher EventQueue Size").__().
            __().
            __().
            tbody().$class("ui-widget-content").
            tr().
            td(String.valueOf("SC-1")).
            td(String.valueOf("Capacity Scheduler")).
            td("[memory-mb (unit=Mi), vcores]").
            td("<memory:1024, vCores:1>").
            td("<memory:8192, vCores:4>").
            td("50").
            td("0").
            td("200").
            td("100").
            __().
            __().__();

    div.__();
  }
}

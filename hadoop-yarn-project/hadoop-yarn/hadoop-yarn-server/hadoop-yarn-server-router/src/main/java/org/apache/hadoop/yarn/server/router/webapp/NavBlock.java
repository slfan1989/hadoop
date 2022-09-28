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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

/**
 * Navigation block for the Router Web UI.
 */
public class NavBlock extends HtmlBlock {

  @Override
  public void render(Block html) {

    Hamlet.UL<Hamlet.DIV<Hamlet>> mainList = html.div("#nav").
        h3("Cluster").
        ul().
        li().a(url(""), "About").__().
        li().a(url("federation"), "Federation").__();

    Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> subAppsList1 =
        mainList.li().a(url("nodes"), "Nodes").ul();
    subAppsList1.li().__();
    subAppsList1.
            li().a(url("nodes", "SC-1"), "SC-1").__();
    subAppsList1.
            li().a(url("nodes", "SC-2"), "SC-2").__();
    subAppsList1.__().__();

    Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> subAppsList2 =
        mainList.li().a(url("apps"), "Applications").ul();
    subAppsList2.li().__();
    subAppsList2.
            li().a(url("nodes", "SC-1"), "SC-1").__();
    subAppsList2.
            li().a(url("nodes", "SC-2"), "SC-2").__();
    subAppsList2.__().__();

    Hamlet.DIV<Hamlet> sectionBefore = mainList.__();
    Configuration conf = new Configuration();
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(sectionBefore, conf);

    if (tools == null) {
      return;
    }

    tools.__().__();

    /*li().a(url("apps"), "Applications").__().
      __().
      h3("Tools").
      ul().li().a("/conf", "Configuration").__().li().a("/logs", "Local logs").__().
        li().a("/stacks", "Server stacks").__().
        li().a("/jmx?qry=Hadoop:*", "Server metrics").__().__().__();*/
  }
}

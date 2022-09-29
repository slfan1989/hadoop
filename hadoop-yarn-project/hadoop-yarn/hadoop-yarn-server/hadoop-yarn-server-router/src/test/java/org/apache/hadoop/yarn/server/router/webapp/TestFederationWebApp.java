/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;

import java.io.IOException;

public class TestFederationWebApp {

  @Test
  public void testFederationWebViewNotEnable()
      throws InterruptedException, YarnException, IOException {
    // Test Federation is not Enabled
    Configuration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.FEDERATION_ENABLED, false);
    WebAppTests.testPage(FederationPage.class, Router.class, new MockRouter(config));
  }

  @Test
  public void testFederationWebViewEnable()
      throws InterruptedException, YarnException, IOException {
    // Test Federation Enabled
    Configuration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    WebAppTests.testPage(FederationPage.class, Router.class, new MockRouter(config));
  }
}

package org.apache.hadoop.yarn.server.router.webapp.dao;

import javax.xml.bind.annotation.*;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;

import java.util.ArrayList;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationClusterInfo extends ClusterInfo {

  @XmlElement(name = "subCluster")
  private ArrayList<ClusterInfo> list = new ArrayList<>();

  public FederationClusterInfo() {
  } // JAXB needs this

  public FederationClusterInfo(ArrayList<ClusterInfo> list) {
    this.list = list;
  }
}

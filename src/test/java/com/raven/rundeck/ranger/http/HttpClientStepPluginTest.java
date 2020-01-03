package com.raven.rundeck.ranger.http;

import com.dtolabs.rundeck.core.execution.workflow.steps.PluginStepContextImpl;
import com.dtolabs.rundeck.core.execution.workflow.steps.StepException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.serviceprovider.ServiceProvider;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import java.util.HashMap;
import java.util.Map;

public class HttpClientStepPluginTest {

  /**
   * Setup options for simple execution for the given method using OAuth 2.0.
   *
   * @param method HTTP Method to use.
   * @return Options for the execution.
   */
  public Map<String, Object> getOptions(String method, String zk) {
    Map<String, Object> options = new HashMap<>();
    options.put("zkConnectionString", zk);
    options.put("namespace", "test");
    options.put("service", "test");
    options.put("environment", "test");
    options.put("method", method);
    options.put("secured", false);
    options.put("uri", "/test");
    options.put("acceptHeader", "application/json");
    options.put("contentTypeHeader", "application/json");
    return options;
  }

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(18089);

  private HttpClient plugin;

  private ServiceProvider<ShardInfo> service;

  private TestingCluster server;

  private CuratorFramework curatorFramework;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Before
  public void setUp() throws Exception {
    plugin = new HttpClient();
    // Simple endpoint
    WireMock.stubFor(WireMock.request("GET", WireMock.urlEqualTo("/test")).atPriority(100)
        .willReturn(WireMock.aResponse()
            .withStatus(200)));
    server = new TestingCluster(1);
    server.start();

    curatorFramework = CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .namespace("test")
        .retryPolicy(new RetryNTimes(5, 1000))
        .zk34CompatibilityMode(true)
        .build();


    curatorFramework.start();

    service = ServiceProviderBuilders.<ShardInfo>shardedServiceProviderBuilder()
        .withCuratorFramework(curatorFramework)
        .withHostname("127.0.0.1")
        .withNamespace("test")
        .withPort(18089)
        .withServiceName("test")
        .withNodeData(ShardInfo.builder()
            .environment("test")
            .build())
        .withHealthcheck( () -> HealthcheckStatus.healthy)
        .withSerializer(serviceNode -> {
          try {
            return objectMapper.writeValueAsBytes(serviceNode);
          } catch (JsonProcessingException e) {
            return null;
          }
        })
        .buildServiceDiscovery();
    service.start();
  }

  @After
  public void tearDown() throws Exception {
    service.stop();
    curatorFramework.close();
    server.close();
  }


  @Test()
  public void testHttpGet() throws StepException {
    this.plugin.setCuratorFramework(curatorFramework);
    this.plugin.executeStep(new PluginStepContextImpl(), this.getOptions("GET", server.getConnectString()));
    verify(1,  getRequestedFor(urlEqualTo("/test")));
  }

}

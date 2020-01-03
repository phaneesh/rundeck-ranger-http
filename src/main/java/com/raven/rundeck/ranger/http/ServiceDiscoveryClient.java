package com.raven.rundeck.ranger.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceFinderBuilders;
import com.flipkart.ranger.finder.sharded.SimpleShardedServiceFinder;
import com.flipkart.ranger.model.ServiceNode;
import lombok.Builder;
import lombok.extern.log4j.Log4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.util.List;
import java.util.Optional;

/**
 * Client that returns a healthy node from nodes for a particular environment
 */
@Log4j
public class ServiceDiscoveryClient {
  private final ShardInfo criteria;
  private SimpleShardedServiceFinder<ShardInfo> serviceFinder;

  @Builder(builderMethodName = "fromConnectionString", builderClassName = "FromConnectionStringBuilder")
  private ServiceDiscoveryClient(
      String namespace,
      String serviceName,
      String environment,
      ObjectMapper objectMapper,
      String connectionString,
      int refreshTimeMs,
      boolean disableWatchers) {
    this(namespace,
        serviceName,
        environment,
        objectMapper,
        CuratorFrameworkFactory.newClient(connectionString, new RetryForever(5000)),
        refreshTimeMs,
        disableWatchers);
  }

  @Builder(builderMethodName = "fromCurator", builderClassName = "FromCuratorBuilder")
  ServiceDiscoveryClient(
      String namespace,
      String serviceName,
      String environment,
      ObjectMapper objectMapper,
      CuratorFramework curator,
      int refreshTimeMs,
      boolean disableWatchers) {

    int effectiveRefreshTimeMs = refreshTimeMs;
    if (effectiveRefreshTimeMs < 5000) {
      effectiveRefreshTimeMs = 5000;
      log.warn("Node info update interval too low: " +refreshTimeMs +" ms. Has been upgraded to 5000ms");
    }

    this.criteria = ShardInfo.builder()
        .environment(environment)
        .build();
    this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
        .withCuratorFramework(curator)
        .withNamespace(namespace)
        .withServiceName(serviceName)
        .withDeserializer(data -> {
          try {
            return objectMapper.readValue(data,
                new TypeReference<ServiceNode<ShardInfo>>() {
                });
          } catch (Exception e) {
            log.warn("Could not parse node data", e);
          }
          return null;
        })
        .withNodeRefreshIntervalMs(effectiveRefreshTimeMs)
        .withDisableWatchers(disableWatchers)
        .build();
  }

  public void start() throws Exception {
    serviceFinder.start();
  }

  public void stop() throws Exception {
    serviceFinder.stop();
  }

  public Optional<ServiceNode<ShardInfo>> getNode() {
    return Optional.ofNullable(serviceFinder.get(criteria));
  }

  public List<ServiceNode<ShardInfo>> getAllNodes() {
    return serviceFinder.getAll(criteria);
  }

}
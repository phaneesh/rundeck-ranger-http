package com.raven.rundeck.ranger.http;

import com.dtolabs.rundeck.core.execution.workflow.steps.StepException;
import com.dtolabs.rundeck.core.execution.workflow.steps.StepFailureReason;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.core.plugins.configuration.Describable;
import com.dtolabs.rundeck.core.plugins.configuration.Description;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.step.PluginStepContext;
import com.dtolabs.rundeck.plugins.step.StepPlugin;
import com.dtolabs.rundeck.plugins.util.DescriptionBuilder;
import com.dtolabs.rundeck.plugins.util.PropertyBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.model.ServiceNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Setter;
import lombok.val;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.http.HttpHeaders;

import java.util.Map;
import java.util.Optional;

@Plugin(name = "com.raven.rundeck.ranger.http.HttpClient", service = ServiceNameConstants.WorkflowStep)
public class HttpClient implements StepPlugin, Describable {

  private static final Log log = LogFactory.getLog(HttpClient.class);

  private ServiceDiscoveryClient serviceDiscoveryClient;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Setter
  private CuratorFramework curatorFramework;

  private static OkHttpClient httpClient;

  public Description getDescription() {
    return DescriptionBuilder.builder()
        .name("com.raven.rundeck.ranger.http.HttpClient")
        .title("Ranger Http Call")
        .description("Makes a HTTP call using ranger service discovery")
        .property(PropertyBuilder.builder()
            .string("zkConnectionString")
            .title("Zookeeper Connection")
            .description("Zookeeper Connection String")
            .required(true)
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("namespace")
            .title("Namespace")
            .description("Namespace for service discovery")
            .required(true)
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("service")
            .title("Service")
            .description("Name of the service")
            .required(true)
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("environment")
            .title("Environment")
            .description("Service discovery environment")
            .required(true)
            .build()
        )
        .property(PropertyBuilder.builder()
            .select("method")
            .title("Method")
            .description("HTTP Method")
            .required(true)
            .defaultValue("GET")
            .values("GET", "POST", "PUT", "DELETE")
            .build()
        )
        .property(PropertyBuilder.builder()
            .booleanType("secured")
            .title("Secured")
            .description("Secured Service")
            .required(false)
            .defaultValue("false")
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("uri")
            .title("URI")
            .description("URI")
            .required(true)
            .defaultValue("/")
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("payload")
            .title("Request Payload")
            .description("Payload that needs to be sent with request")
            .required(false)
            .defaultValue(null)
            .renderingAsTextarea()
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("user")
            .title("User Name")
            .description("User Name")
            .required(false)
            .defaultValue(null)
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("password")
            .title("Password")
            .description("Password")
            .required(false)
            .defaultValue(null)
            .renderingAsPassword()
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("authorization")
            .title("Authorization Token")
            .description("Authorization Token/Header that will be sent (Specify with prefix)")
            .required(false)
            .defaultValue(null)
            .renderingAsPassword()
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("acceptHeader")
            .title("Accept Header")
            .description("Accept Header, Defaulted to application/json")
            .required(false)
            .defaultValue("application/json")
            .build()
        )
        .property(PropertyBuilder.builder()
            .string("contentTypeHeader")
            .title("Content Type Header")
            .description("Content Type Header, Defaulted to application/json")
            .required(false)
            .defaultValue("application/json")
            .build()
        )
        .build();
  }

  @Override
  public void executeStep(PluginStepContext pluginStepContext, Map<String, Object> map) throws StepException {
    try {
      log.info("Initializing ranger HTTP call");
      init(map);
      log.info("Initialized ranger HTTP call");
      log.info("Executing ranger HTTP call");
      execute(map);
      log.info("Completed executing ranger HTTP call");
    } catch (Exception e) {
      log.error("Error executing HTTP ranger call", e);
      throw new StepException(e, StepFailureReason.ConfigurationFailure);
    }
  }


  private synchronized void init(Map<String, Object> map) throws Exception {
    String namespace = String.valueOf(map.get("namespace"));
    String service = String.valueOf(map.get("service"));
    String environment = String.valueOf(map.get("environment"));
    String zkConnectionString = String.valueOf(map.get("zkConnectionString"));
    log.info("Ranger Call configuration [ ZK Connection String: " +zkConnectionString +" | Namespace: " +namespace +" | Service: " +service +" | Environment: " +environment);
    if (curatorFramework == null) {
      curatorFramework = CuratorFrameworkFactory.builder()
          .namespace(namespace)
          .zk34CompatibilityMode(true)
          .connectString(zkConnectionString)
          .retryPolicy(new RetryNTimes(5000, 1000))
          .build();
      curatorFramework.start();
    }
    if (httpClient == null) {
      String user = String.valueOf(map.get("user"));
      String password = String.valueOf(map.get("password"));
      String authorization = String.valueOf(map.get("authorization"));
      if (!Strings.isNullOrEmpty(user) & Strings.isNullOrEmpty(authorization)) {
        httpClient = new OkHttpClient.Builder()
            .followRedirects(true)
            .hostnameVerifier((s, sslSession) -> true)
            .authenticator((route, response) -> {
              String credentials = Credentials.basic(user, password);
              return response.request().newBuilder()
                  .addHeader(HttpHeaders.AUTHORIZATION, credentials).build();
            })
            .build();
      } else if (!Strings.isNullOrEmpty(authorization)) {
        httpClient = new OkHttpClient.Builder()
            .followRedirects(true)
            .hostnameVerifier((s, sslSession) -> true)
            .authenticator((route, response) -> response.request().newBuilder()
                .addHeader(HttpHeaders.AUTHORIZATION, authorization).build())
            .build();
      } else {
        httpClient = new OkHttpClient.Builder()
            .followRedirects(true)
            .hostnameVerifier((s, sslSession) -> true)
            .build();
      }
    }
    serviceDiscoveryClient = ServiceDiscoveryClient.fromCurator()
        .curator(curatorFramework)
        .disableWatchers(true)
        .namespace(namespace)
        .environment(environment)
        .serviceName(service)
        .refreshTimeMs(10000)
        .objectMapper(OBJECT_MAPPER)
        .build();
    serviceDiscoveryClient.start();
  }

  protected void execute(Map<String, Object> map) throws Exception {
    Preconditions.checkNotNull(httpClient);
    String method = String.valueOf(map.get("method"));
    switch (method) {
      case "POST":
        doPost(map);
        break;
      case "PUT":
        doPut(map);
        break;
      case "DELETE":
        doDelete(map);
        break;
      default:
        doGet(map);
    }
  }

  private Response doGet(Map<String, Object> map) throws Exception {
    Request.Builder httpRequest = initializeRequest(map);
    httpRequest.get();
    return executeRequest(httpRequest.build());
  }

  private Response doDelete(Map<String, Object> map) throws Exception {
    Request.Builder httpRequest = initializeRequest(map);
    httpRequest.delete();
    return executeRequest(httpRequest.build());
  }

  private Response doPost(Map<String, Object> map) throws Exception {
    Request.Builder httpRequest = initializeRequest(map);
    if (map.get("payload") != null) {
      httpRequest.post(
          RequestBody.create(MediaType.parse(String.valueOf(map.get("contentTypeHeader"))), String.valueOf(map.get("payload"))));
    } else {
      httpRequest.post(RequestBody.create(MediaType.parse("*/*"), new byte[0]));
    }
    return executeRequest(httpRequest.build());
  }

  private Response doPut(Map<String, Object> map) throws Exception {
    Request.Builder httpRequest = initializeRequest(map);
    if (map.get("payload") != null) {
      httpRequest.post(
          RequestBody.create(MediaType.parse(String.valueOf(map.get("contentTypeHeader"))), String.valueOf(map.get("payload"))));
    } else {
      httpRequest.put(RequestBody.create(MediaType.parse("*/*"), new byte[0]));
    }
    return executeRequest(httpRequest.build());
  }

  private HttpUrl generateURI(Map<String, Object> map) {
    val builder = new HttpUrl.Builder();
    String secured = String.valueOf(map.get("secured"));
    if ("true".equals(secured)) {
      builder.scheme("https");
    } else {
      builder.scheme("http");
    }
    Optional<ServiceNode<ShardInfo>> node = serviceDiscoveryClient.getNode();
    if (node.isPresent()) {
      builder.host(node.get().getHost()).port(node.get().getPort())
          .encodedPath(String.valueOf(map.get("uri")));
    } else {
      log.error("Error executing ranger call! No node available");
      throw new IllegalStateException("Service unavailable");
    }
    return builder.build();
  }

  private Request.Builder initializeRequest(Map<String, Object> map) {
    val url = generateURI(map);
    log.info("Ranger call URL: " + url);
    val httpRequest = new Request.Builder().url(url);
    httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, String.valueOf(map.get("contentTypeHeader")));
    httpRequest.addHeader(HttpHeaders.ACCEPT, String.valueOf(map.get("acceptHeader")));
    return httpRequest;
  }

  private Response executeRequest(Request request) throws Exception {
    Response response = null;
    try {
      response = httpClient.newCall(request).execute();
      return response;
    } catch (Exception e) {
      log.error("Error executing service request for service : " + request.url(), e);
      throw e;
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
}

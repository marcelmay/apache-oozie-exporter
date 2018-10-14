package de.m3y.prometheus.exporter.oozie;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class WebServerIT {
    private Server server;
    private String exporterBaseUrl;
    private HttpClient client;

    @Before
    public void setUp() throws Exception {
        Config config = new Config();
        config.oozieApiUrl = "http://localhost:11000/oozie/";
        config.listenerHost = "localhost";
        config.listenerPort = 7772;

        server = new WebServer().configure(config).start();
        exporterBaseUrl = "http://localhost:7772";
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        client = httpClientBuilder.build();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testMetrics() throws Exception {
        HttpResponse response = getResponse(exporterBaseUrl + "/metrics");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String body = EntityUtils.toString(response.getEntity());

        // App info
        assertTrue(body.contains("oozie_exporter_app_info{appName=\"oozie_exporter\",appVersion=\""));

        // JVM GC Info
        assertTrue(body.contains("jvm_memory_pool_bytes_used{"));
        assertTrue(body.contains("jvm_memory_bytes_used{"));

        // Test welcome page
        response = getResponse(exporterBaseUrl);
        assertEquals(200, response.getStatusLine().getStatusCode());
        body = EntityUtils.toString(response.getEntity());
        assertTrue(body.contains("Apache Oozie Exporter"));
        assertTrue(body.contains("SCM branch"));
        assertTrue(body.contains("SCM version"));
        assertTrue(body.contains("Metrics"));
    }

    private HttpResponse getResponse(String url) throws IOException {
        final HttpGet request = new HttpGet(url);
        return client.execute(request);
    }
}

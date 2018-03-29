package de.m3y.prometheus.exporter.oozie;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import okhttp3.*;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClient.Metrics;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects stats from Apache Oozie via
 * <a href="http://oozie.apache.org/docs/4.2.0/WebServicesAPI.html#Oozie_Metrics">metrics</a> or
 * <a href="http://oozie.apache.org/docs/4.2.0/WebServicesAPI.html#Oozie_Instrumentation">instrumentation</a> API.
 * <p>
 * Note: Oozie unfortunately computes statistics, instead of providing the raw values.
 */
public class OozieCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(OozieCollector.class);

    private static final String METRIC_PREFIX = "oozie_";

    private static final Counter METRIC_SCRAPE_REQUESTS = Counter.build()
            .name(METRIC_PREFIX + "scrape_requests_total")
            .help("Exporter requests made")
            .labelNames("oozie_api")
            .register();
    private static final Counter METRIC_SCRAPE_ERROR = Counter.build()
            .name(METRIC_PREFIX + "scrape_errors_total")
            .help("Counts failed scrapes.")
            .labelNames("oozie_api")
            .register();

    private static final Gauge METRIC_SCRAPE_DURATION = Gauge.build()
            .name(METRIC_PREFIX + "scrape_duration_seconds")
            .help("Scrape duration")
            .labelNames("oozie_api")
            .register();

    static class OozieClientHack extends OozieClient {
        public Metrics createMetrics(JSONObject json) {
            return new Metrics(json); // Requires enclosing OozieClient, unfortunately.
        }

        public Instrumentation createInstrumentation(JSONObject json) {
            return new Instrumentation(json);
        }
    }

    private static final OozieClientHack OOZIE_CLIENT_HACK = new OozieClientHack();

    static abstract class AbstractOozieCollector extends Collector {
        final OkHttpClient httpClient;
        final Request request;
        final String apiLabel;

        protected AbstractOozieCollector(String apiLabel, OkHttpClient httpClient, Request request) {
            this.httpClient = httpClient;
            this.request = request;
            this.apiLabel = apiLabel;
        }

        JSONObject parseJsonObject(Request apiRequest) {
            try {
                Response response = httpClient.newCall(apiRequest).execute();
                String body = response.body().string();
                return (JSONObject) JSONValue.parse(body);
            } catch (IOException e) {
                throw new IllegalStateException("Can not invoke/parse call to " + apiRequest.url(), e);
            }
        }

        @Override
        public List<MetricFamilySamples> collect() {
            try (Gauge.Timer timer = METRIC_SCRAPE_DURATION.labels(apiLabel).startTimer()) {
                METRIC_SCRAPE_REQUESTS.labels(apiLabel).inc();
                scrape();
            } catch (Exception e) {
                METRIC_SCRAPE_ERROR.labels(apiLabel).inc();
                LOGGER.error("Scrape failed", e);
            }
            return Collections.EMPTY_LIST;
        }

        protected abstract void scrape();

        boolean isAvailable() {
            try {
                final Response response = httpClient.newCall(request).execute();
                LOGGER.info("Checking availability of " + request.url() + " : " + response.code());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info("Result fetched is: " + response.body().string());
                }
                return response.code() == 200;
            } catch (IOException e) {
                return false;
            }
        }

       static void addGauges(Gauge gauge, Map<String, ?> gauges, String group) {
            for (Entry<String, ?> gaugeEntry : gauges.entrySet()) {
                final Object value = gaugeEntry.getValue();
                if (value instanceof Number) {
                    String key = gaugeEntry.getKey();
                    int idx = key.indexOf('.');
                    if (idx > 0) {
                        String varType = key.substring(0, idx);
                        String varName = key.substring(idx + 1);
                        gauge.labels(varType, varName).set(((Number) value).doubleValue());
                    } else {
                        LOGGER.warn("Not supported : Ignoring oozie variable without group.name pattern : " + key);
                    }
                } else if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Ignoring unsupported type {} of {} : {} with value  {}",
                            gaugeEntry.getValue().getClass(),
                            group, gaugeEntry.getKey(), gaugeEntry.getValue()
                    );
                }
            }
        }

        // TODO: should be counter instead of gauge, but counter can not set() value
        static void addCounters(Gauge gauge, Map<String, Long> counters) {
            for (Entry<String, Long> counterEntry : counters.entrySet()) {
                // Example : jpa.GET_RUNNING_ACTIONS
                final String key = counterEntry.getKey();
                int idx = key.indexOf('.');
                if (idx > 0) {
                    String counterType = key.substring(0, idx);
                    String counterName = key.substring(idx + 1);
                    gauge.labels(counterType, counterName).set(counterEntry.getValue());
                } else {
                    LOGGER.warn("Not supported : oozie counter without counter type part in key " + key);
                }
            }
        }
    }

    /**
     * Collects http://oozie.apache.org/docs/4.2.0/WebServicesAPI.html#Oozie_Instrumentation metrics.
     */
    static class OozieAdminInstrumentationCollector extends AbstractOozieCollector {
        private static final String ADMIN_INSTRUMENTATION = "admin_instrumentation";
        private static final String ADMIN_INSTRUMENTATION_PREFIX = METRIC_PREFIX + ADMIN_INSTRUMENTATION + "_";
        private static final Gauge INSTRUMENTATION_TIMER_OWN = Gauge.build()
                .name(ADMIN_INSTRUMENTATION_PREFIX + "timer_own_seconds")
                .help("Oozie timers: <Own> time spent on various Oozie internal operations")
                .labelNames("timer_type", "timer_name", "timer_stat")
                .register();
        private static final Gauge INSTRUMENTATION_TIMER_TOTAL = Gauge.build()
                .name(ADMIN_INSTRUMENTATION_PREFIX + "timer_total_seconds")
                .help("Oozie timers: <Total> time spent on various Oozie internal operations")
                .labelNames("timer_type", "timer_name", "timer_stat")
                .register();
        private static final Gauge INSTRUMENTATION_TIMER_TICKS = Gauge.build()
                .name(ADMIN_INSTRUMENTATION_PREFIX + "timer_ticks_total")
                .help("Oozie timers: Various Oozie internal operation ticks")
                .labelNames("timer_type", "timer_name")
                .register();
        private static final Gauge INSTRUMENTATION_VARIABLES = Gauge.build()
                .name(ADMIN_INSTRUMENTATION_PREFIX + "variable")
                .help("Oozie variables: Oozie internal vars (numerics only)")
                .labelNames("var_group", "var_name")
                .register();
        //       Change to counter_total, if using Prometheus Counter instead of Gauge is possible
        private static final Gauge INSTRUMENTATION_COUNTER = Gauge.build()
                .name(ADMIN_INSTRUMENTATION_PREFIX + "counter")
                .help("Oozie counters")
                .labelNames("counter_type", "counter_name").register();

        OozieAdminInstrumentationCollector(OkHttpClient httpClient, Config config) {
            super(ADMIN_INSTRUMENTATION,
                    httpClient,
                    new Request.Builder()
                            .url(config.oozieApiUrl + '/' + RestConstants.ADMIN + '/' + RestConstants.ADMIN_INSTRUMENTATION_RESOURCE)
                            .build());
        }

        @Override
        public void scrape() {
            final OozieClient.Instrumentation instrumentation = getInstrumentation();
            addCounters(INSTRUMENTATION_COUNTER, instrumentation.getCounters());
            addGauges(INSTRUMENTATION_VARIABLES, instrumentation.getSamplers(), "samplers");
            addGauges(INSTRUMENTATION_VARIABLES, instrumentation.getVariables(), "variables");
            addInstrumentationTimers(instrumentation.getTimers());
        }

        public OozieClient.Instrumentation getInstrumentation() {
            JSONObject json = parseJsonObject(request);
            fixBrokenOozieInstrumentationJson(json, "");
            return OOZIE_CLIENT_HACK.createInstrumentation(json);
        }


        private void addInstrumentationTimers(Map<String, OozieClient.Instrumentation.Timer> timers) {
            for (Entry<String, OozieClient.Instrumentation.Timer> timerEntry : timers.entrySet()) {
                final OozieClient.Instrumentation.Timer value = timerEntry.getValue();

                final String key = timerEntry.getKey();
                int idx = key.indexOf('.');
                if (idx > 0) {
                    String timerType = key.substring(0, idx);
                    String timerName = key.substring(idx + 1);
                    INSTRUMENTATION_TIMER_TOTAL.labels(timerType, timerName, "std_dev")
                            .set(value.getTotalTimeStandardDeviation() / 1000d /* Convert ms to seconds */);
                    INSTRUMENTATION_TIMER_TOTAL.labels(timerType, timerName, "avg").set(value.getTotalTimeAverage() / 1000d);
                    INSTRUMENTATION_TIMER_TOTAL.labels(timerType, timerName, "min").set(value.getTotalMinTime() / 1000d);
                    INSTRUMENTATION_TIMER_TOTAL.labels(timerType, timerName, "max").set(value.getTotalMaxTime() / 1000d);

                    INSTRUMENTATION_TIMER_OWN.labels(timerType, timerName, "std_dev").set(value.getOwnTimeStandardDeviation() / 1000d);
                    INSTRUMENTATION_TIMER_OWN.labels(timerType, timerName, "avg").set(value.getOwnTimeAverage() / 1000d);
                    INSTRUMENTATION_TIMER_OWN.labels(timerType, timerName, "min").set(value.getOwnMinTime() / 1000d);
                    INSTRUMENTATION_TIMER_OWN.labels(timerType, timerName, "max").set(value.getOwnMaxTime() / 1000d);

                    INSTRUMENTATION_TIMER_TICKS.labels(timerType, timerName).set(value.getTicks());
                } else {
                    LOGGER.warn("Not supported : oozie instrumentation timer without timer type part in key " + key);
                }
            }
        }

        /**
         * Oozie sometimes returns null values ... which its own data classes can not parse.
         * This fixes null values by setting value to 0.
         *
         * @param json the json object.
         * @param path the current path.
         */
        private void fixBrokenOozieInstrumentationJson(JSONObject json, String path) {
            Set<Map.Entry<String, Object>> entries = json.entrySet();
            for (Entry<String, Object> entry : entries) {
                Object value = entry.getValue();
                String currentPath = path + "/" + entry.getKey();
                if (value instanceof JSONObject) {
                    fixBrokenOozieInstrumentationJson((JSONObject) value, currentPath);
                } else if (value instanceof JSONArray) {
                    JSONArray array = (JSONArray) value;
                    for (int i = 0; i < array.size(); i++) {
                        fixBrokenOozieInstrumentationJson((JSONObject) array.get(i), currentPath + "[" + i + "]");
                    }
                } else {
                    if (null == value) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Fixing " + currentPath + " with value " + entry.getKey());
                        }
                        entry.setValue(0);
                    }
                }
            }

        }
    }

    /**
     * Collects http://oozie.apache.org/docs/4.2.0/WebServicesAPI.html#Oozie_Metrics .
     * <p>
     * Note: Does currently not support Timers and Histogram.
     */
    static class OozieAdminMetricsCollector extends AbstractOozieCollector {
        private static final String ADMIN_METRICS = "admin_metrics";
        private static final String ADMIN_METRICS_PREFIX = METRIC_PREFIX + ADMIN_METRICS + "_";

        private static final Gauge METRICS_VARIABLES = Gauge.build()
                .name(ADMIN_METRICS_PREFIX + "variable")
                .help("Oozie variables: Oozie internal vars (numerics only)")
                .labelNames("var_group", "var_name")
                .register();
        //       Change to counter_total, if using Prometheus Counter instead of Gauge is possible
        private static final Gauge METRICS_COUNTER = Gauge.build()
                .name(ADMIN_METRICS_PREFIX + "counter")
                .help("Oozie counters")
                .labelNames("counter_type", "counter_name").register();

        OozieAdminMetricsCollector(OkHttpClient httpClient, Config config) {
            super(ADMIN_METRICS,
                    httpClient,
                    new Request.Builder()
                            .url(config.oozieApiUrl + '/' + RestConstants.ADMIN + '/' + RestConstants.ADMIN_METRICS_RESOURCE)
                            .build());
        }

        @Override
        public void scrape() {
            final Metrics metrics = getMetrics();
            addCounters(METRICS_COUNTER, metrics.getCounters());
            addGauges(METRICS_VARIABLES, metrics.getGauges(), "gauges");
            //                metrics.getHistograms() TODO!
            //                addMetricsTimers(mfs, metrics.getTimers()); TODO!
        }

        public Metrics getMetrics() {
            JSONObject json = parseJsonObject(request);
            return OOZIE_CLIENT_HACK.createMetrics(json);
        }
    }

    OozieCollector(Config config) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting Oozie exporter with Oozie API base URL  " + config.oozieApiUrl);
        }

        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        if (config.skipHttpsVerification) {
            disableHttpsVerification(builder);
        }
        if (config.hasOozieAuthentication()) {
            builder = builder.authenticator((route, response) -> {
                String credential = Credentials.basic(config.oozieUser, config.ooziePassword);
                return response.request().newBuilder().header("Authorization", credential).build();
            });
        }
        OkHttpClient httpClient = builder.build();

        final OozieAdminInstrumentationCollector adminInstrumentationCollector =
                new OozieAdminInstrumentationCollector(httpClient, config);
        if (adminInstrumentationCollector.isAvailable()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Registering Oozie admin instrumentation collector");
            }
            adminInstrumentationCollector.register();
        }

        final OozieAdminMetricsCollector adminMetricsCollector = new OozieAdminMetricsCollector(httpClient, config);
        if (adminMetricsCollector.isAvailable()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Registering Oozie admin metrics collector");
            }
            adminMetricsCollector.register();
        }
    }

    private void disableHttpsVerification(OkHttpClient.Builder builder) {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }
        }};

        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, null);
            builder.sslSocketFactory(sc.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
            HostnameVerifier trustAnyHostnameVerifier = (host, session) -> true;
            builder.hostnameVerifier(trustAnyHostnameVerifier);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<MetricFamilySamples> collect() {
        // Already registered specific collectors.
        return Collections.emptyList();
    }


    private static final Pattern PATTERN_INVALID_METRIC_NAME_CHARS = Pattern.compile("[.\\-#]");

    static String escapeName(String name) {
        return METRIC_PREFIX + PATTERN_INVALID_METRIC_NAME_CHARS.matcher(name).replaceAll("_");
    }
}


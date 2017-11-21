package de.m3y.prometheus.exporter.oozie;

import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.regex.Pattern;
import javax.net.ssl.*;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.oozie.client.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects stats from Apache Oozie via
 * <a href="http://oozie.apache.org/docs/4.2.0/WebServicesAPI.html#Oozie_Metrics">metrics</a> or
 * <a href="http://oozie.apache.org/docs/4.2.0/WebServicesAPI.html#Oozie_Instrumentation">instrumentation</a> API.
 */
public class OozieCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(OozieCollector.class);

    private static final String METRIC_PREFIX = "oozie_";

    private static final Counter METRIC_SCRAPE_REQUESTS = Counter.build()
            .name(METRIC_PREFIX + "scrape_requests_total")
            .help("Exporter requests made").register();
    private static final Counter METRIC_SCRAPE_ERROR = Counter.build()
            .name(METRIC_PREFIX + "scrape_errors_total")
            .help("Counts failed scrapes.").register();

    private static final Gauge METRIC_SCRAPE_DURATION = Gauge.build()
            .name(METRIC_PREFIX + "scrape_duration_seconds")
            .help("Scrape duration").register();

    private final OozieClient oozieClient; // Thread safe, according to Javadocs

    OozieCollector(Config config) {
        if (config.skipHttpsVerification) {
            disableHttpsVerification();
        }

        oozieClient = new OozieClient(config.oozieApiUrl);

        if (config.hasOozieAuthentication()) {
            final String credentials = config.oozieUser + ":" + config.ooziePassword;
            String credentialsBase64Encoded = Base64.getEncoder().encodeToString(credentials
                    .getBytes(StandardCharsets.US_ASCII));
            oozieClient.setHeader("Authorization", "Basic " + credentialsBase64Encoded);
        }
    }

    private void disableHttpsVerification() {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }
        }};

        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalStateException(e);
        }

        HostnameVerifier trustAnyHostnameVerifier = (host, session) -> true;
        HttpsURLConnection.setDefaultHostnameVerifier(trustAnyHostnameVerifier);
    }

    public List<MetricFamilySamples> collect() {
        try (Gauge.Timer timer = METRIC_SCRAPE_DURATION.startTimer()) {
            METRIC_SCRAPE_REQUESTS.inc();

            final OozieClient.Metrics metrics = oozieClient.getMetrics();
            if (null != metrics) {
                addCounters(metrics.getCounters());
                addGauges(metrics.getGauges(), "gauges");
//                metrics.getHistograms() TODO!
//                addMetricsTimers(mfs, metrics.getTimers()); TODO!
            } else {
                final OozieClient.Instrumentation instrumentation = oozieClient.getInstrumentation();
                addCounters(instrumentation.getCounters());
                addGauges(instrumentation.getSamplers(), "samplers");
                addGauges(instrumentation.getVariables(), "variables");
                addInstrumentationTimers(instrumentation.getTimers());
            }
        } catch (Exception e) {
            METRIC_SCRAPE_ERROR.inc();
            LOGGER.error("Scrape failed", e);
        }

        return Collections.emptyList();
    }

//    private void addMetricsTimers(List<MetricFamilySamples> mfs, Map<String, OozieClient.Metrics.Timer> timers) {
//        for (Map.Entry<String, OozieClient.Metrics.Timer> timerEntry : timers.entrySet()) {
//            final OozieClient.Metrics.Timer value = timerEntry.getValue();
//            final String namePrefix = createName(timerEntry.getKey());
//            TODO !!!
//        }
//        LOGGER.warn("Not yet impl! TODO!!!");
//    }


    static final Gauge TIMER_OWN = Gauge.build()
            .name(METRIC_PREFIX + "timer_own_seconds")
            .help("Oozie timers: <Own> time spent on various Oozie internal operations")
            .labelNames("timer_type", "timer_name", "timer_stat")
            .register();
    static final Gauge TIMER_TOTAL = Gauge.build()
            .name(METRIC_PREFIX + "timer_total_seconds")
            .help("Oozie timers: <Total> time spent on various Oozie internal operations")
            .labelNames("timer_type", "timer_name", "timer_stat")
            .register();
    static final Gauge TIMER_TICKS = Gauge.build()
            .name(METRIC_PREFIX + "timer_ticks_total")
            .help("Oozie timers: Various Oozie internal operation ticks")
            .labelNames("timer_type", "timer_name")
            .register();

    private void addInstrumentationTimers(Map<String, OozieClient.Instrumentation.Timer> timers) {
        for (Map.Entry<String, OozieClient.Instrumentation.Timer> timerEntry : timers.entrySet()) {
            final OozieClient.Instrumentation.Timer value = timerEntry.getValue();

            final String key = timerEntry.getKey();
            int idx = key.indexOf('.');
            if (idx > 0) {
                String timerType = key.substring(0, idx);
                String timerName = key.substring(idx + 1);
                TIMER_TOTAL.labels(timerType, timerName, "std_dev")
                        .set(value.getTotalTimeStandardDeviation() / 1000d /* Convert ms to seconds */);
                TIMER_TOTAL.labels(timerType, timerName, "avg").set(value.getTotalTimeAverage() / 1000d);
                TIMER_TOTAL.labels(timerType, timerName, "min").set(value.getTotalMinTime() / 1000d);
                TIMER_TOTAL.labels(timerType, timerName, "max").set(value.getTotalMaxTime() / 1000d);

                TIMER_OWN.labels(timerType, timerName, "std_dev").set(value.getOwnTimeStandardDeviation() / 1000d);
                TIMER_OWN.labels(timerType, timerName, "avg").set(value.getOwnTimeAverage() / 1000d);
                TIMER_OWN.labels(timerType, timerName, "min").set(value.getOwnMinTime() / 1000d);
                TIMER_OWN.labels(timerType, timerName, "max").set(value.getOwnMaxTime() / 1000d);

                TIMER_TICKS.labels(timerType, timerName).set(value.getTicks());
            } else {
                LOGGER.warn("Not supported : oozie instrumentation timer without timer type part in key " + key);
            }
        }
    }

    static final Gauge VARIABLES = Gauge.build()
            .name(METRIC_PREFIX + "variables")
            .help("Oozie variables: Oozie internal vars (numerics only)")
            .labelNames("var_group", "var_name")
            .register();

    private void addGauges(Map<String, ?> gauges, String group) {
        for (Map.Entry<String, ?> gaugeEntry : gauges.entrySet()) {
            final Object value = gaugeEntry.getValue();
            if (value instanceof Number) {
                String key = gaugeEntry.getKey();
                int idx = key.indexOf('.');
                if (idx > 0) {
                    String varType = key.substring(0, idx);
                    String varName = key.substring(idx + 1);
                    VARIABLES.labels(varType, varName).set(((Number) value).doubleValue());
                } else {
                    LOGGER.warn("Not supported : Ignoring oozie instrumentation variable without group.name pattern : " + key);
                }
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignoring unsupported type {} of {} : {} with value  {}",
                        gaugeEntry.getValue().getClass(),
                        group, gaugeEntry.getKey(), gaugeEntry.getValue()
                );
            }
        }
    }

    static final Gauge COUNTER = Gauge.build() // TODO: should be counter, but counter can not set() value
            .name(METRIC_PREFIX + "counter")   //       Change to counter_total, if using Prometheus Counter instead of Gauge is possible
            .help("Oozie counters")
            .labelNames("counter_type", "counter_name").register();

    private void addCounters(Map<String, Long> counters) {
        for (Map.Entry<String, Long> counterEntry : counters.entrySet()) {
            // Example : jpa.GET_RUNNING_ACTIONS
            final String key = counterEntry.getKey();
            int idx = key.indexOf('.');
            if (idx > 0) {
                String counterType = key.substring(0, idx);
                String counterName = key.substring(idx + 1);
                COUNTER.labels(counterType, counterName).set(counterEntry.getValue());
            } else {
                LOGGER.warn("Not supported : oozie counter without counter type part in key " + key);
            }
        }
    }

    private static final Pattern PATTERN_INVALID_METRIC_NAME_CHARS = Pattern.compile("[.\\-#]");

    static String escapeName(String name) {
        return METRIC_PREFIX + PATTERN_INVALID_METRIC_NAME_CHARS.matcher(name).replaceAll("_");
    }
}


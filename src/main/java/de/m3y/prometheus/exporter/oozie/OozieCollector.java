package de.m3y.prometheus.exporter.oozie;

import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.net.ssl.*;

import io.prometheus.client.*;
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

    static final String METRIC_PREFIX = "oozie_";

    private static final Counter METRIC_SCRAPE_REQUESTS = Counter.build()
            .name(METRIC_PREFIX + "scrape_requests_total")
            .help("Exporter requests made").register();
    private static final Counter METRIC_SCRAPE_ERROR = Counter.build()
            .name(METRIC_PREFIX + "scrape_errors_total")
            .help("Counts failed scrapes.").register();

    private static final Gauge METRIC_SCRAPE_DURATION = Gauge.build()
            .name(METRIC_PREFIX + "scrape_duration_seconds")
            .help("Scrape duration").register();

    final OozieClient oozieClient; // Thread safe, according to Javadocs

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

        HostnameVerifier trustAnyHostnameVerifier = new HostnameVerifier() {
            public boolean verify(String host, SSLSession session) {
                return true;
            }
        };
        HttpsURLConnection.setDefaultHostnameVerifier(trustAnyHostnameVerifier);
    }

    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
        try (Gauge.Timer timer = METRIC_SCRAPE_DURATION.startTimer()) {
            METRIC_SCRAPE_REQUESTS.inc();

            final OozieClient.Metrics metrics = oozieClient.getMetrics();
            if (null != metrics) {
                addCounters(mfs, metrics.getCounters(), "counters");
                addGauges(mfs, metrics.getGauges(),"gauges");
//                metrics.getHistograms() TODO!
//                addMetricsTimers(mfs, metrics.getTimers()); TODO!
            } else {
                final OozieClient.Instrumentation instrumentation = oozieClient.getInstrumentation();
                addCounters(mfs, instrumentation.getCounters(), "counters");
                addGauges(mfs, instrumentation.getSamplers(), "samplers");
                addGauges(mfs, instrumentation.getVariables(), "variables");
                addInstrumentationTimers(mfs, instrumentation.getTimers(), "timers");
            }
        } catch (Exception e) {
            METRIC_SCRAPE_ERROR.inc();
            LOGGER.error("Scrape failed", e);
        }

        return mfs;
    }

//    private void addMetricsTimers(List<MetricFamilySamples> mfs, Map<String, OozieClient.Metrics.Timer> timers) {
//        for (Map.Entry<String, OozieClient.Metrics.Timer> timerEntry : timers.entrySet()) {
//            final OozieClient.Metrics.Timer value = timerEntry.getValue();
//            final String namePrefix = createName(timerEntry.getKey());
//            TODO !!!
//        }
//        LOGGER.warn("Not yet impl! TODO!!!");
//    }

    private void addInstrumentationTimers(List<MetricFamilySamples> mfs, Map<String, OozieClient.Instrumentation.Timer> timers, String group) {
        for (Map.Entry<String, OozieClient.Instrumentation.Timer> timerEntry : timers.entrySet()) {
            final OozieClient.Instrumentation.Timer value = timerEntry.getValue();
            final String namePrefix = createName(group + "_" + timerEntry.getKey());
            mfs.add(new GaugeMetricFamily(namePrefix + "_own_max_time", "", value.getOwnMaxTime()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_own_min_time", "", value.getOwnMinTime()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_own_avg_time", "", value.getOwnTimeAverage()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_own_std_dev_time", "", value.getOwnTimeStandardDeviation()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_ticks", "", value.getTicks()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_total_max_time", "", value.getTotalMaxTime()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_total_min_time", "", value.getTotalMinTime()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_total_avg_time", "", value.getTotalTimeAverage()));
            mfs.add(new GaugeMetricFamily(namePrefix + "_total_std_dev_time", "", value.getTotalTimeStandardDeviation()));
        }
    }

    private void addGauges(List<MetricFamilySamples> mfs, Map<String, ?> gauges,String group) {
        for (Map.Entry<String, ?> gaugeEntry : gauges.entrySet()) {
            final Object value = gaugeEntry.getValue();
            if (value instanceof Number) {
                mfs.add(new GaugeMetricFamily(createName(group + "_" + gaugeEntry.getKey()), "", ((Number) value).doubleValue()));
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unhandled {} : {} with value {} of type {}",
                        group, gaugeEntry.getKey(), gaugeEntry.getValue(), gaugeEntry.getValue().getClass());
            }
        }
    }

    private void addCounters(List<MetricFamilySamples> mfs, Map<String, Long> counters, String group) {
        for (Map.Entry<String, Long> counterEntry : counters.entrySet()) {
            mfs.add(new CounterMetricFamily(createName(group + "_" + counterEntry.getKey()), "", counterEntry.getValue()));
        }
    }

    private static final Pattern PATTERN_INVALID_METRIC_NAME_CHARS = Pattern.compile("\\.|-|#");

    private static String createName(String key) {
        return METRIC_PREFIX + PATTERN_INVALID_METRIC_NAME_CHARS.matcher(key).replaceAll("_");
    }
}


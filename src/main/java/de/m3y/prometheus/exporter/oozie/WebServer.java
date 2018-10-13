package de.m3y.prometheus.exporter.oozie;

import java.net.InetSocketAddress;
import java.util.Arrays;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.log4j.Level;
import org.apache.log4j.spi.RootLogger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class WebServer {

    private Server server;

    WebServer configure(Config config) {
        DefaultExports.initialize();

        final OozieCollector oozieCollector = new OozieCollector(config);
        oozieCollector.register();

        final BuildInfoExporter buildInfo = new BuildInfoExporter("oozie_exporter_",
                "oozie_exporter").register();

        // Jetty
        InetSocketAddress inetAddress = new InetSocketAddress(config.listenerHost, config.listenerPort);
        server = new Server(inetAddress);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        context.addServlet(new ServletHolder(new HomePageServlet(config, buildInfo)), "/");

        return this;
    }

    Server start() throws Exception {
        server.start();
        return server;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Expected more arguments, got " + Arrays.toString(args));
            printUsageAndExit();
        }

        Config config = parseArgs(args);

        new WebServer().
                configure(config).
                start().
                join();
    }

    private static void printUsageAndExit() {
        System.err.println();
        System.err.println("Usage: WebServer <OPTIONS>");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println("    -web.listen-address=[<hostname>:]<port>  Exporter listener address"); // NOSONAR
        System.err.println("    -oozie.url=<Oozie API Url>               Oozie API oozieApiUrl, eg http://localhost:11000/oozie"); // NOSONAR
        System.err.println("    [-oozie.user=<USER>]                     Oozie API user for authentication");
        System.err.println("    [-oozie.password=<PWD>]                  Oozie API password for authentication");
        System.err.println("    [-oozie.password.env=<ENV VAR>]          Env var containing Oozie API password for authentication");
        System.err.println("    [-skipHttpsVerification]                 Skip SSL/TLS verification for Oozie HTTPS URL"); // NOSONAR
        System.err.println("    [-Dlog.level=[DEBUG|INFO|WARN|ERROR]]    Sets the log level. Defaults to INFO"); // NOSONAR
        System.err.println();
        System.exit(1);
    }

    private static Config parseArgs(String[] args) {
        RootLogger.getRootLogger().setLevel(Level.toLevel(System.getProperty("log.level"), Level.INFO));

        Config config = new Config();
        for (String arg : args) {
            if (arg.equals("-skipHttpsVerification")) {
                config.skipHttpsVerification = true;
            } else if (arg.startsWith("-web.listen-address=")) {
                final String value = arg.substring("-web.listen-address=".length());
                String[] parts = value.split(":");
                if (parts.length == 1) {
                    config.listenerPort = Integer.parseInt(parts[0]);
                    config.listenerHost = "0.0.0.0";
                } else if (parts.length == 2) {
                    config.listenerPort = Integer.parseInt(parts[1]);
                    config.listenerHost = parts[0];
                } else {
                    System.err.println("Can not extract host from -web.listen-address=" +
                            value + ", expected <[ip:]port>");
                    printUsageAndExit();
                }
            } else if (arg.startsWith("-oozie.url=")) {
                config.oozieApiUrl = arg.substring("-oozie.url=".length());
            } else if (arg.startsWith("-oozie.user=")) {
                config.oozieUser = arg.substring("-oozie.user=".length());
            } else if (arg.startsWith("-oozie.password=")) {
                config.ooziePassword = arg.substring("-oozie.password=".length());
            } else if (arg.startsWith("-oozie.password.env=")) {
                config.ooziePassword = System.getenv(arg.substring("-oozie.password.env=".length()));
            } else {
                System.err.println("Unknown option <" + arg + "> in " + Arrays.toString(args));
                printUsageAndExit();
            }
        }
        return config;
    }
}

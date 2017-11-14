package de.m3y.prometheus.exporter.oozie;

/**
 * Configuration options.
 */
public class Config {
    String oozieApiUrl;
    public boolean skipHttpsVerification;
    public int listenerPort;
    public String listenerHost;
    public String oozieUser;
    public String ooziePassword;

    public boolean hasOozieAuthentication() {
        return null!=oozieUser && oozieUser.length()>0;
    }
}

package de.m3y.prometheus.exporter.oozie;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WebServerTest {
    @Test
    public void testEscapeName() {
        assertEquals("oozie_foo_bar_baz_nop", OozieCollector.escapeName("foo-bar.baz#nop"));
    }
}

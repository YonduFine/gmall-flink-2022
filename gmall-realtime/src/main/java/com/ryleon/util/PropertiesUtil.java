package com.ryleon.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public class PropertiesUtil {
    private final static String propertiesFilePath = "config.properties";

    public static Properties getProperties() {
        Properties properties = new Properties();
        try {
            properties.load(
                new InputStreamReader(
                    Objects.requireNonNull(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFilePath)),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
}

package org.jgroups.raft.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.jcip.annotations.Immutable;

@Immutable
public interface RuntimeProperties {

    String PROPERTY_PREFIX = "jgroups.raft";

    /**
     * Create a new {@link Builder configuration builder}.
     *
     * @return the configuration builder
     */
    static Builder create() {
        return new Builder();
    }

    /**
     * Create a new {@link Builder configuration builder} that starts with a copy of the supplied configuration.
     *
     * @param config the configuration to copy; may be null
     * @return the configuration builder
     */
    static Builder copy(RuntimeProperties config) {
        return config != null ? new Builder(config.asProperties()) : new Builder();
    }

    /**
     * Obtain a configuration instance by copying the supplied Properties object. The supplied {@link Properties} object is
     * copied so that the resulting Configuration cannot be modified.
     *
     * @param properties the properties; may be null or empty
     * @return the configuration; never null
     */
    static RuntimeProperties from(Properties properties) {
        Properties props = new Properties();
        if (properties != null) {
            properties.stringPropertyNames().forEach(key -> props.setProperty(key.trim(), properties.getProperty(key)));
        }
        return new RuntimeProperties() {
            @Override
            public String getString(String key) {
                return props.getProperty(key);
            }

            @Override
            public Set<String> keys() {
                return props.stringPropertyNames();
            }

            @Override
            public String toString() {
                return asProperties().toString();
            }
        };
    }

    /**
     * Obtain a configuration instance by copying the supplied map of string keys and object values. The entries within the map
     * are copied so that the resulting Configuration cannot be modified.
     *
     * @param properties the properties; may be null or empty
     * @return the configuration; never null
     */
    static RuntimeProperties from(Map<String, ?> properties) {
        return from(properties, value -> {
            if (value == null) {
                return null;
            }
            if (value instanceof Collection<?> c) {
                return c.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","));
            }
            return value.toString();
        });
    }

    /**
     * Obtain a configuration instance by copying the supplied map of string keys and object values. The entries within the map
     * are copied so that the resulting Configuration cannot be modified.
     *
     * @param properties the properties; may be null or empty
     * @param conversion the function that converts the supplied values into strings, or returns {@code null} if the value
     *            is to be excluded
     * @return the configuration; never null
     */
    static <T> RuntimeProperties from(Map<String, T> properties, Function<T, String> conversion) {
        Map<String, T> props = new HashMap<>();
        if (properties != null) {
            props.putAll(properties);
        }
        return new RuntimeProperties() {
            @Override
            public String getString(String key) {
                return conversion.apply((T) props.get(key));
            }

            @Override
            public Set<String> keys() {
                return props.keySet();
            }

            @Override
            public String toString() {
                return asProperties().toString();
            }
        };
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied URL.
     *
     * @param url the URL to the stream containing the configuration properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    static RuntimeProperties load(URL url) throws IOException {
        try (InputStream stream = url.openStream()) {
            return load(stream);
        }
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied file.
     *
     * @param file the file containing the configuration properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    static RuntimeProperties load(File file) throws IOException {
        try (InputStream stream = new FileInputStream(file)) {
            return load(stream);
        }
    }

    /**
     * Obtain a configuration instance by loading the Properties from the supplied stream.
     *
     * @param stream the stream containing the properties; may not be null
     * @return the configuration; never null
     * @throws IOException if there is an error reading the stream
     */
    static RuntimeProperties load(InputStream stream) throws IOException {
        try (stream) {
            Properties properties = new Properties();
            properties.load(stream);
            return from(properties);
        }
    }

    interface ConfigurationBuilder<C extends RuntimeProperties, B extends ConfigurationBuilder<C, B>> {

        /**
         * Build and return the immutable configuration.
         *
         * @return the immutable configuration; never null
         */
        C build();

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        B with(String key, String value);

        /**
         * Apply the function to this builder.
         *
         * @param function the predefined field for the key
         * @return this builder object so methods can be chained together; never null
         */
        B apply(Consumer<B> function);

        /**
         * Remove the value associated with the specified key.
         *
         * @param key the key
         * @return this builder object so methods can be chained together; never null
         */
        B without(String key);

        /**
         * Add all of the fields in the supplied Configuration object.
         *
         * @param other the configuration whose fields should be added; may not be null
         * @return this builder object so methods can be chained together; never null
         */
        default B with(RuntimeProperties other) {
            return apply(builder -> other.forEach(builder::with));
        }

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, int value) {
            return with(key, Integer.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, float value) {
            return with(key, Float.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, double value) {
            return with(key, Double.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, long value) {
            return with(key, Long.toString(value));
        }

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, boolean value) {
            return with(key, Boolean.toString(value));
        }

        /**
         * Associate the given class name value with the specified key.
         *
         * @param key the key
         * @param value the Class value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, Class<?> value) {
            return with(key, value != null ? value.getName() : null);
        }

        /**
         * Associate the given value with the specified key.
         *
         * @param key the key
         * @param value the value
         * @return this builder object so methods can be chained together; never null
         */
        default B with(String key, Object value) {
            return with(key, value != null ? value.toString() : null);
        }
    }

    /**
     * Get the set of keys in this configuration.
     *
     * @return the set of keys; never null but possibly empty
     */
    Set<String> keys();

    /**
     * Get the string value associated with the given key.
     *
     * @param key the key for the configuration property
     * @return the value, or null if the key is null or there is no such key-value pair in the configuration
     */
    String getString(String key);

    /**
     * Get the boolean value associated with the given field when that field has a default value. If the configuration does
     * not have a name-value pair with the same name as the field, then the field's default value.
     *
     * @param property the property
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration and there is
     *         no default value in the field or the default value could not be parsed as a long, or there is a key-value pair in
     *         the configuration but the value could not be parsed as a boolean value
     * @throws NumberFormatException if there is no name-value pair and the field has no default value
     */
    default boolean getBoolean(Property property) {
        return getBoolean(property.name(), () -> Boolean.parseBoolean(property.defaultValueAsString()));
    }

    /**
     * Get the boolean value associated with the given key, using the given supplier to obtain a default value if there is no such
     * key-value pair.
     *
     * @param key the key for the configuration property
     * @param defaultValueSupplier the supplier for the default value; may be null
     * @return the boolean value, or null if the key is null, there is no such key-value pair in the configuration, the
     *         {@code defaultValueSupplier} reference is null, or there is a key-value pair in the configuration but the value
     *         could not be parsed as a boolean value
     */
    default Boolean getBoolean(String key, BooleanSupplier defaultValueSupplier) {
        String value = getString(key);
        if (value != null) {
            value = value.trim().toLowerCase();
            if (Boolean.valueOf(value)) {
                return Boolean.TRUE;
            }
            if (value.equals("false")) {
                return false;
            }
        }
        return defaultValueSupplier != null ? defaultValueSupplier.getAsBoolean() : null;
    }

    /**
     * Get a copy of these configuration properties as a Properties object.
     *
     * @return the properties object; never null
     */
    default Properties asProperties() {
        Properties props = new Properties();
        // Add all values as-is ...
        keys().forEach(key -> {
            String value = getString(key);
            if (key != null && value != null) {
                props.setProperty(key, value);
            }
        });
        return props;
    }

    /**
     * Get a copy of these configuration properties as a Properties object.
     *
     * @return the properties object; never null
     */
    default Map<String, String> asMap() {
        Map<String, String> props = new HashMap<>();
        // Add all values as-is ...
        keys().forEach(key -> {
            String value = getString(key);
            if (key != null && value != null) {
                props.put(key, value);
            }
        });
        return props;
    }

    /**
     * Call the supplied function for each of the fields.
     *
     * @param function the consumer that takes the field name and the string value extracted from the field; may
     *            not be null
     */
    default <T> void forEach(BiConsumer<String, String> function) {
        this.asMap().forEach(function);
    }

    class Builder implements ConfigurationBuilder<RuntimeProperties, Builder> {
        private final Properties properties = new Properties();

        private Builder() { }

        private Builder(Properties properties) {
            this.properties.putAll(properties);
        }

        @Override
        public Builder with(String key, String value) {
            if (!properties.contains(key))
                properties.setProperty(key, value);
            return this;
        }

        @Override
        public Builder without(String key) {
            properties.remove(key);
            return this;
        }

        @Override
        public Builder apply(Consumer<Builder> function) {
            function.accept(this);
            return this;
        }

        @Override
        public RuntimeProperties build() {
            return RuntimeProperties.from(properties);
        }
    }
}

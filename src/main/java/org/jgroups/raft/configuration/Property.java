package org.jgroups.raft.configuration;

import java.util.Objects;
import java.util.function.Supplier;

import net.jcip.annotations.Immutable;

@Immutable
public final class Property {

    private final String name;
    private final String displayName;
    private final String description;
    private final Supplier<Object> defaultValue;

    public Property(String name, String displayName, String description, Supplier<Object> defaultValue) {
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public static Builder create(String name) {
        return new Builder(name);
    }

    /**
     * Get the name of the field.
     * @return the name; never null
     */
    public String name() {
        return name;
    }

    /**
     * Get the default value of the field.
     * @return the default value, or {@code null} if there is no default value
     */
    public Object defaultValue() {
        return defaultValue.get();
    }

    /**
     * Get the string representation of the default value of the field.
     * @return the default value, or {@code null} if there is no default value
     */
    public String defaultValueAsString() {
        Object defaultValue = defaultValue();
        return defaultValue != null ? defaultValue.toString() : null;
    }

    /**
     * Get the description of the field.
     * @return the description; never null
     */
    public String description() {
        return description;
    }

    /**
     * Get the display name of the field.
     * @return the display name; never null
     */
    public String displayName() {
        return displayName;
    }

    public static final class Builder {
        private final String name;
        private String displayName;
        private String description;
        private Supplier<Object> defaultValue;

        private Builder(String name) {
            this.name = Objects.requireNonNull(name, "property name can not be null");
        }

        public Builder withDisplayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withDefaultValue(Object value) {
            this.defaultValue = () -> value;
            return this;
        }

        public Builder withDefaultValue(Supplier<Object> provider) {
            this.defaultValue = provider;
            return this;
        }

        public Property build() {
            return new Property(name, displayName, description, defaultValue);
        }
    }
}

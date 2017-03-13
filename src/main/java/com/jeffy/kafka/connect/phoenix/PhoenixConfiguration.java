
/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
    
package com.jeffy.kafka.connect.phoenix;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.util.Collect;

/**
 * @Author Jeffy
 * @Email: renwu58@gmail.com
 * 
 */
public interface PhoenixConfiguration extends Configuration{
    /**
     * A field for the name of the database. This field has no default value.
     */
    public static final Field DATABASE = Field.create("dbname",
                                                      "Name of the database");
    /**
     * A field for the user of the database. This field has no default value.
     */
    public static final Field USER = Field.create("user",
                                                  "Name of the database user to be used when connecting to the database");
    /**
     * A field for the password of the database. This field has no default value.
     */
    public static final Field PASSWORD = Field.create("password",
                                                      "Password to be used when connecting to the database");
    /**
     * A field for the hostname of the database server. This field has no default value.
     */
    public static final Field JDBC_URL = Field.create("jdbc.connection", "JDBC connection url for phoenix database");

    /**
     * The set of names of the pre-defined JDBC configuration fields, including {@link #DATABASE}, {@link #USER},
     * {@link #PASSWORD}, and {@link #JDBC_URL}.
     */
    public static Set<String> ALL_KNOWN_FIELDS = Collect.unmodifiableSet(Field::name, DATABASE, USER, PASSWORD, JDBC_URL);
    /**
     * Obtain a {@link PhoenixConfiguration} adapter for the given {@link Configuration}.
     * 
     * @param config the configuration; may not be null
     * @return the ClientConfiguration; never null
     */
    public static PhoenixConfiguration adapt(Configuration config) {
        if (config instanceof PhoenixConfiguration) return (PhoenixConfiguration) config;
        return new PhoenixConfiguration() {
            @Override
            public Set<String> keys() {
                return config.keys();
            }

            @Override
            public String getString(String key) {
                return config.getString(key);
            }

            @Override
            public String toString() {
                return config.toString();
            }
        };
    }
    public static interface Builder extends Configuration.ConfigBuilder<PhoenixConfiguration, Builder>{
        
        default Builder withDatabase(String database){
            return with(DATABASE, database);
        }
        
        default Builder withUser(String user){
            return with(USER, user);
        }
        
        default Builder withPassword(String password){
            return with(PASSWORD, password);
        }
        
        default Builder withJdbcUrl(String jdbcUrl){
            return with(JDBC_URL, jdbcUrl);
        }
        
    }
    /**
     * Create a new {@link Builder configuration builder} that starts with a copy of the supplied configuration.
     * 
     * @param config the configuration to copy
     * @return the configuration builder
     */
    public static Builder copy(Configuration config) {
        return new Builder() {
            private Configuration.Builder builder = Configuration.copy(config);

            @Override
            public Builder with(String key, String value) {
                builder.with(key, value);
                return this;
            }

            @Override
            public Builder withDefault(String key, String value) {
                builder.withDefault(key, value);
                return this;
            }

            @Override
            public Builder apply(Consumer<Builder> function) {
                function.accept(this);
                return this;
            }
            
            @Override
            public Builder changeString(Field field, Function<String, String> function) {
                changeString(field,function);
                return this;
            }
            
            @Override
            public Builder changeString(String key, Function<String, String> function) {
                changeString(key,function);
                return this;
            }

            @Override
            public PhoenixConfiguration build() {
                return PhoenixConfiguration.adapt(builder.build());
            }

            @Override
            public String toString() {
                return builder.toString();
            }
        };
    }

    
    /**
     * Create a new {@link Builder configuration builder} that starts with an empty configuration.
     * 
     * @return the configuration builder
     */
    public static Builder create() {
        return new Builder() {
            private Configuration.Builder builder = Configuration.create();

            @Override
            public Builder with(String key, String value) {
                builder.with(key, value);
                return this;
            }

            @Override
            public Builder withDefault(String key, String value) {
                builder.withDefault(key, value);
                return this;
            }

            @Override
            public Builder apply(Consumer<Builder> function) {
                function.accept(this);
                return this;
            }
            
            @Override
            public Builder changeString(Field field, Function<String, String> function) {
                changeString(field,function);
                return this;
            }
            
            @Override
            public Builder changeString(String key, Function<String, String> function) {
                changeString(key,function);
                return this;
            }

            @Override
            public PhoenixConfiguration build() {
                return PhoenixConfiguration.adapt(builder.build());
            }

            @Override
            public String toString() {
                return builder.toString();
            }
        };
    }

    /**
     * Get a predicate that determines if supplied keys are pre-defined field names.
     * 
     * @return the predicate; never null
     */
    default Predicate<String> knownFieldNames() {
        return ALL_KNOWN_FIELDS::contains;
    }

    /**
     * Get a view of this configuration that does not contain the {@link #knownFieldNames() known fields}.
     * 
     * @return the filtered view of this configuration; never null
     */
    default Configuration withoutKnownFields() {
        return filter(knownFieldNames().negate());
    }

    /**
     * Get the hostname property from the configuration.
     * 
     * @return the specified or default host name, or null if there is none.
     */
    default String getJdbcUrl() {
        return getString(JDBC_URL);
    }

    /**
     * Get the database name property from the configuration.
     * 
     * @return the specified or default database name, or null if there is none.
     */
    default String getDatabase() {
        return getString(DATABASE);
    }

    /**
     * Get the user property from the configuration.
     * 
     * @return the specified or default username, or null if there is none.
     */
    default String getUser() {
        return getString(USER);
    }

    /**
     * Get the password property from the configuration.
     * 
     * @return the specified or default password value, or null if there is none.
     */
    default String getPassword() {
        return getString(PASSWORD);
    }
}

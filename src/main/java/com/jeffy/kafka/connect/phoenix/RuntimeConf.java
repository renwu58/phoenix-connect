
/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.jeffy.kafka.connect.phoenix;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.Field;

/**
 * @Author Jeffy
 * @Email: renwu58@gmail.com
 * 
 */
public class RuntimeConf {

    // Phoenix 的JDBC连接配置项
    private static final String JDBC_URL_NAME = "jdbc.connection";
    // 数据库用户名
    private static final String DATABASE_USER_NAME = "database.user";
    // 数据库密码
    private static final String DATABASE_PASSWORD_NAME = "database.password";
    // JDBC 提交批次大小
    private static final String BATCH_SIZE_NAME = "batch.size";

    private static final String MAX_RETRIES_NAME = "max.retries";
    private static final String RETRY_BACKOFF_MS_NAME = "retry.backoff.ms";
    private static final String DATABASE_TENANT_IDS_NAME = "database.tenant.ids";
    
    public static final Field JDBC_URL = Field.create(JDBC_URL_NAME)
            .withDescription("JDBC Connection URL")
            .withDisplayName("Phoenix JDBC URL")
            .withImportance(Importance.HIGH)
            .withType(Type.STRING).withWidth(Width.MEDIUM)
            .withValidation(Field::isRequired);

    public static final Field USER = Field.create(DATABASE_USER_NAME)
            .withDisplayName("User")
            .withImportance(Importance.LOW)
            .withType(Type.STRING)
            .withDescription("Name of the Phoenix database user to be used when connecting to the database.");

    public static final Field PASSWORD = Field.create(DATABASE_PASSWORD_NAME)
            .withDisplayName("Password")
            .withImportance(Importance.LOW)
            .withType(Type.PASSWORD)
            .withDescription("Password of the Phoenix database user to be used when connecting to the database.");

    public static final Field BATCH_SIZE = Field.create(BATCH_SIZE_NAME)
            .withDisplayName("JDBC batch size")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum size of each batch of commit. Defaults to 3000.")
            .withDefault(3000)
            .withValidation(Field::isPositiveInteger);

    public static final Field MAX_RETRIES = Field.create(MAX_RETRIES_NAME)
            .withDisplayName("Maximum Retries")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The maximum number of times to retry on errors before failing the task. Defaults to 10.")
            .withDefault(10)
            .withValidation(Field::isPositiveInteger);

    public static final Field RETRY_BACKOFF_MS = Field.create(RETRY_BACKOFF_MS_NAME)
            .withDisplayName("Retry Backoff (millis)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The time in milliseconds to wait following an error before a retry attempt is made. Defaults to 3000ms")
            .withDefault(3000)
            .withValidation(Field::isPositiveInteger);

    public static final Field DATABASE_TENANT_ENABLE = Field.create("database.tenant.enable")
            .withDisplayName("Enable tenant")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether enable mult-tenancy. Defaults to false")
            .withDefault(false)
            .withValidation(Field::isBoolean);
    public static final Field DATABASE_TENANT_IDS = Field.create(DATABASE_TENANT_IDS_NAME)
            .withDisplayName("Tenant ids")
            .withType(Type.LIST)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The identity of the tenant use commas to separate");

    public static final Field DATABASE_SERVER_NAME = Field.create("database.server.name")
            .withDisplayName("Namespace")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Unique name that identifies the database server and all recorded offsets, and"
                    + "that is used as a prefix for all schemas and topics. "
                    + "Each distinct MySQL installation should have a separate namespace"
                    + "Defaults to 'host:port'");

    public static final Field GROUP_ID = Field.create("group.id")
            .withDisplayName("Group id")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Group id for consumers to retrieve all topics, default to server.id");

    public static final Field KEY_DESERIALIZER = Field.create("key.deserializer")
            .withDisplayName("key deserializer")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("org.apache.kafka.common.serialization.StringDeserializer")
            .withValidation(Field::isRequired)
            .withDescription("key deserializer for consumers to retrieve all topics, default to org.apache.kafka.common.serialization.StringDeserializer ");

    public static final Field VALUE_DESERIALIZER = Field.create("value.deserializer")
            .withDisplayName("value deserializer")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("org.apache.kafka.common.serialization.StringDeserializer")
            .withValidation(Field::isRequired)
            .withDescription("Group id for consumers to retrieve all topics, default to server.id");
    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create("kafka.recovery.poll.interval.ms")
            .withDisplayName("Poll interval during progress history recovery (ms)")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
            .withDefault(100)
            .withValidation(Field::isInteger);

    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create("kafka.recovery.attempts")
            .withDisplayName("Max attempts to recovery progress history")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The number of attempts in a row that no data are returned from Kafka before recover completes. "
                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) x (recovery.poll.interval.ms).")
            .withDefault(4)
            .withValidation(Field::isInteger);

    public static Field.Set ALL_FIELDS = Field.setOf(JDBC_URL, USER, PASSWORD,
            BATCH_SIZE, MAX_RETRIES, RETRY_BACKOFF_MS, DATABASE_SERVER_NAME, DATABASE_TENANT_ENABLE, GROUP_ID,
            KEY_DESERIALIZER, VALUE_DESERIALIZER,RECOVERY_POLL_INTERVAL_MS,RECOVERY_POLL_ATTEMPTS);

    public static Field.Set DB_FIELDS = Field.setOf(JDBC_URL,USER,PASSWORD);
    
    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();
        Field.group(config, "Phoenix", JDBC_URL, USER, PASSWORD,DATABASE_TENANT_ENABLE);
        Field.group(config, "connector", BATCH_SIZE, MAX_RETRIES, RETRY_BACKOFF_MS, DATABASE_SERVER_NAME);
        return config;
    }

}

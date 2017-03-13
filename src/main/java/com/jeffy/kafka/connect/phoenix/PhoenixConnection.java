/**
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.kafka.connect.phoenix;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Column;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
/**
 * 对Phoenix 数据的操作使用的连接管理类
 * 
 * @Author Jeffy
 * @Email: renwu58@gmail.com
 */

public class PhoenixConnection implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(PhoenixConnection.class);
    public final static String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //public final static String PHOENIX_DRIVER = "org.apache.phoenix.queryserver.client.Driver";
    private volatile Connection conn;
    private final Configuration config;
    private final ConnectionFactory factory;
    private final Operations initialOps;
    private static Tables tables = null;
    
    
    /**
     * Defines multiple JDBC operations.
     */
    @FunctionalInterface
    public static interface Operations {
        /**
         * Apply a series of operations against the given JDBC statement.
         * 
         * @param statement the JDBC statement to use to execute one or more operations
         * @throws SQLException if there is an error connecting to the database or executing the statements
         */
        void apply(Statement statement) throws SQLException;
    }

    /**
     * Create a new instance with the given configuration and connection factory.
     * 
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     */
    public PhoenixConnection(Configuration config, ConnectionFactory connectionFactory) {
        this(config, connectionFactory, null);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     * 
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     */
    public PhoenixConnection(Configuration config, ConnectionFactory connectionFactory, Operations initialOperations) {
        this(config, connectionFactory, initialOperations, null);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     * 
     * @param config the configuration; may not be null
     * @param connectionFactory the connection factory; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     * @param adapter the function that can be called to update the configuration with defaults
     */
    protected PhoenixConnection(Configuration config, ConnectionFactory connectionFactory, Operations initialOperations,
            Consumer<Configuration.Builder> adapter) {
        this.config = adapter == null ? config : config.edit().apply(adapter).build();
        this.factory = connectionFactory;
        this.initialOps = initialOperations;
        this.conn = null;
    }
    /**
     * Obtain the configuration for this connection.
     * 
     * @return the JDBC configuration; never null
     */
    public PhoenixConfiguration config() {
        return PhoenixConfiguration.adapt(config);
    }
    /**
     * Establishes Phoenix JDBC connections.
     */
    @FunctionalInterface
    @ThreadSafe
    public static interface ConnectionFactory {
        /**
         * Establish a connection to the database denoted by the given configuration.
         * 
         * @param config the configuration with JDBC connection information
         * @return the JDBC connection; may not be null
         * @throws SQLException if there is an error connecting to the database
         */
        Connection connect(PhoenixConfiguration config) throws SQLException;
    }

    /**
     * Create a {@link ConnectionFactory} that replaces variables in the supplied URL pattern. Variables include:
     * <ul>
     * <li><code>${jdbc_url}</code></li>
     * <li><code>${dbname}</code></li>
     * <li><code>${username}</code></li>
     * <li><code>${password}</code></li>
     * </ul>
     * 
     * @param urlPattern the JDBC URL string; may not be null
     * @param variables any custom or overridden configuration variables
     * @return the connection factory
     */
    public static ConnectionFactory BasedConnectionFactory(String urlPattern, Field... variables) {
        return (config) -> {
            log.trace("Config: {}", config.asProperties());
            Properties props = config.asProperties();
            Field[] varsWithDefaults = combineVariables(variables,
                    PhoenixConfiguration.JDBC_URL,
                    PhoenixConfiguration.USER,
                    PhoenixConfiguration.PASSWORD,
                    PhoenixConfiguration.DATABASE);
            String url = findAndReplace(urlPattern,props,varsWithDefaults);
            log.trace("Props: {}", props);
            log.trace("URL: {}", url);
            try {
                Class.forName(PHOENIX_DRIVER);
            } catch (ClassNotFoundException e) { //如果找不到默认的驱动，给出警告
                log.warn("Cannot found default JDBC Driver: " + PHOENIX_DRIVER, e);
            }
            Connection conn = DriverManager.getConnection(url, props);
            log.debug("Connected to {} with {}", url, props);
            return conn;
        };
    }
    /**
     * 覆写默认值
     * @param overriddenVariables
     * @param defaultVariables
     * @return Field[]
     */
    private static Field[] combineVariables(Field[] overriddenVariables,
            Field... defaultVariables) {
        Map<String, Field> fields = new HashMap<>();
        if (defaultVariables != null) {
            for (Field variable : defaultVariables) {
                fields.put(variable.name(), variable);
            }
        }
        if (overriddenVariables != null) {
            for (Field variable : overriddenVariables) {
                fields.put(variable.name(), variable);
            }
        }
        return fields.values().toArray(new Field[fields.size()]);
    }
    /**
     * 根据配置参数，替换url中部分变量
     * @param url 带有参数化的url
     * @param props 参数配置
     * @param variables 参数
     * @return JDBC URL
     */
    private static String findAndReplace(String url, Properties props, Field... variables) {
        for (Field field : variables) {
            String variable = field.name();
            if (variable != null && url.contains("${" + variable + "}")) {
                // Otherwise, we have to remove it from the properties ...
                String value = props.getProperty(variable);
                if (value != null) {
                    props.remove(variable);
                    // And replace the variable ...
                    url = url.replaceAll("\\$\\{" + variable + "\\}", value);
                }
            }
        }
        return url;
    }

    public static interface ResultSetConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    public static interface SingleParameterResultSetConsumer {
        boolean accept(String parameter, ResultSet rs) throws SQLException;
    }

    public static interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }
    
    /**
     * A function to create a statement from a connection.
     * @author Randall Hauch
     */
    @FunctionalInterface
    public interface StatementFactory {
        /**
         * Use the given connection to create a statement.
         * @param connection the JDBC connection; never null
         * @return the statement
         * @throws SQLException if there are problems creating a statement
         */
        Statement createStatement(Connection connection) throws SQLException;
    }

    /**
     * Execute a SQL query.
     * 
     * @param query the SQL query
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public PhoenixConnection query(String query, ResultSetConsumer resultConsumer) throws SQLException {
        return query(query,conn->conn.createStatement(),resultConsumer);
    }

    /**
     * Execute a SQL query.
     * 
     * @param query the SQL query
     * @param statementFactory the function that should be used to create the statement from the connection; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public PhoenixConnection query(String query, StatementFactory statementFactory, ResultSetConsumer resultConsumer) throws SQLException {
        //Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (log.isTraceEnabled()) {
                log.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }
    /**
     * Execute a SQL prepared query.
     * 
     * @param preparedQueryString the prepared query string
     * @param preparer the function that supplied arguments to the prepared statement; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public PhoenixConnection prepareQuery(String preparedQueryString, StatementPreparer preparer, ResultSetConsumer resultConsumer)
            throws SQLException {
        //Connection conn = connection();
        try (PreparedStatement statement = conn.prepareStatement(preparedQueryString);) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                if (resultConsumer != null) resultConsumer.accept(resultSet);
            }
        }
        return this;
    }

    /**
     * Execute a SQL prepared query.
     * 
     * @param preparedQueryString the prepared query string
     * @param parameters the collection of values for the first and only parameter in the query; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public PhoenixConnection prepareQuery(String preparedQueryString, Collection<String> parameters,
                                       SingleParameterResultSetConsumer resultConsumer)
            throws SQLException {
        return prepareQuery(preparedQueryString, parameters.stream(), resultConsumer);
    }

    /**
     * Execute a SQL prepared query.
     * 
     * @param preparedQueryString the prepared query string
     * @param parameters the stream of values for the first and only parameter in the query; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public PhoenixConnection prepareQuery(String preparedQueryString, Stream<String> parameters,
                                       SingleParameterResultSetConsumer resultConsumer)
            throws SQLException {
        //Connection conn = connection();
        try (PreparedStatement statement = conn.prepareStatement(preparedQueryString);) {
            for (Iterator<String> iter = parameters.iterator(); iter.hasNext();) {
                String value = iter.next();
                statement.setString(1, value);
                boolean success = false;
                try (ResultSet resultSet = statement.executeQuery();) {
                    if (resultConsumer != null) {
                        success = resultConsumer.accept(value, resultSet);
                        if (!success) break;
                    }
                }
            }
        }
        return this;
    }
    /**
     * 建立初始化连接
     * 
     * @return PhoenixConnection
     * @throws SQLException 
     */
    public PhoenixConnection connect() throws SQLException {
        connection();
        return this;
    }

    /**
     * 开启事务
     * 
     * @return PhoenixConnection
     * @throws SQLException 
     */
    public PhoenixConnection begin() throws SQLException {
        if (connection().getAutoCommit()) {
            conn.setAutoCommit(false);
        }
        return this;
    }

    /**
     * Execute a series of SQL statements as a single transaction.
     * 
     * @param sqlStatements the SQL statements that are to be performed as a single transaction
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public PhoenixConnection execute(String... sqlStatements) throws SQLException {
        return execute(statement -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("executing '{}'", sqlStatement);
                    }
                    statement.execute(sqlStatement);
                }
            }
        });
    }

    public PreparedStatement createPreparedStatement(String sql) throws SQLException{
    	return conn.prepareStatement(sql);
    }
    
    /**
     * Execute a series of operations as a single transaction.
     * 
     * @param operations the function that will be called with a newly-created {@link Statement}, and that performs
     *            one or more operations on that statement object
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     */
    public PhoenixConnection execute(Operations operations) throws SQLException {
        //Connection conn = connection();
        try (Statement statement = conn.createStatement();) {
            operations.apply(statement);
            if (!conn.getAutoCommit())
                conn.commit();
        }
        return this;
    }
    /**
     * 提交事务
     * 
     * @return PhoenixConnection
     * @throws SQLException
     */
    public PhoenixConnection commit() throws SQLException {
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
        return this;
    }

    @Override
    public void close() throws SQLException {
        if (conn != null) {
            try {
                conn.close();
            } finally {
                conn = null;
            }
        }
    }

    /**
     * 判断当前是否已经连接成功
     * 
     * @return boolean
     * @throws SQLException
     */
    public synchronized boolean isConnected() throws SQLException {
        if (conn == null)
            return false;
        return !conn.isClosed();
    }

    /**
     * 获取JDBC连接
     * 
     * @return Connection
     * @throws SQLException
     */
    public synchronized Connection connection() throws SQLException {
        if (conn == null) {
            conn = factory.connect(PhoenixConfiguration.adapt(config));
            if (conn == null)
                throw new SQLException("Unable to obtain a JDBC connection");
            // Always run the initial operations on this new connection
            if (initialOps != null)
                execute(initialOps);
        }
        return conn;
    }

    /**
     * Get the names of all of the catalogs.
     * @return the set of catalog names; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<String> readAllCatalogNames()
            throws SQLException {
        Set<String> catalogs = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getCatalogs()) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                catalogs.add(catalogName);
            }
        }
        return catalogs;
    }

    /**
     * Get the names of all of the schemas.
     * @return the set of schemas names; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<String> readAllSchemaNames()
            throws SQLException {
        Set<String> schemas = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getSchemas()) {
            while (rs.next()) {
                String schemaName = rs.getString(1);
                schemas.add(schemaName);
            }
        }
        return schemas;
    }
    
    /**
     * Get the identifiers of all available tables.
     * 
     * @param tableTypes the set of table types to include in the results, which may be null for all table types
     * In Phoenix 4.8 Only supported table types include: INDEX, SEQUENCE, SYSTEM TABLE, TABLE, VIEW
     * @return the set of {@link TableId}s; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<TableId> readAllTableNames(String[] tableTypes) throws SQLException {
        return readTableNames(null, null, null, tableTypes);
    }

    /**
     * Get the identifiers of the tables.
     * 
     * @param databaseCatalog the name of the catalog, which is typically the database name; may be an empty string for tables
     *            that have no catalog, or {@code null} if the catalog name should not be used to narrow the list of table
     *            identifiers
     * @param schemaNamePattern the pattern used to match database schema names, which may be "" to match only those tables with
     *            no schema or {@code null} if the schema name should not be used to narrow the list of table
     *            identifiers
     * @param tableNamePattern the pattern used to match database table names, which may be null to match all table names
     * @param tableTypes the set of table types to include in the results, which may be null for all table types
     * @return the set of {@link TableId}s; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {
        if (tableNamePattern == null) tableNamePattern = "%";
        Set<TableId> tableIds = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, tableNamePattern, tableTypes)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                tableIds.add(tableId);
            }
        }
        return tableIds;
    }
    /**
     * 执行从Phoenix中初始化对应实例的表结构信息到内存中
     * 
     * @param tableSchema
     * @throws SQLException
     */
    public void initTables(String tableSchema) throws SQLException{
        PhoenixConnection.tables = getTables(tableSchema);
    }
    
    public Tables getTables(String tableSchema) throws SQLException{
        /**
         * 通过查询Phoenix的元数据表，获取对应的表信息
         * Schema
         * table
         * column[pk,column,type,nullable,default,size]
         * 
         */
        if (tableSchema == null) {
            tableSchema = "%";
        }
        String getColumnMetadataSQL ="SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_SIZE,DECIMAL_DIGITS,NULLABLE,ORDINAL_POSITION,KEY_SEQ FROM SYSTEM.CATALOG where COLUMN_NAME is not null and TABLE_SCHEM like '"+
                                     tableSchema.toUpperCase() +"' order by TABLE_SCHEM,TABLE_NAME,ORDINAL_POSITION";
        Tables tables = new Tables();
        
        connect().query(getColumnMetadataSQL, (rs)->{
            while (rs.next()) {
              //创建TableId对象
                TableId tableId = new TableId(null, rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
                //根据TableId对象新建或者更新表定义
                TableEditor editor = tables.editOrCreateTable(tableId)
                      .addColumn(
                              Column.editor()
                                    .name(rs.getString("COLUMN_NAME"))
                                    .jdbcType(rs.getInt("DATA_TYPE"))
                                    .length(rs.getInt("COLUMN_SIZE"))
                                    .scale(rs.getInt("DECIMAL_DIGITS"))
                                    .position(rs.getInt("ORDINAL_POSITION"))
                                    .optional(rs.getBoolean("NULLABLE")).create());
                //指定主键
                if (rs.getInt("KEY_SEQ") >0) {
                    editor.setPrimaryKeyNames(rs.getString("COLUMN_NAME"));
                }
                tables.overwriteTable(editor.create());
                //rs.getString("TABLE_SCHEM");
                //rs.getString("TABLE_NAME");           
//                rs.getString("COLUMN_NAME");
//                rs.getInt("DATA_TYPE");
//                rs.getInt("COLUMN_SIZE");
//                rs.getInt("DECIMAL_DIGITS");
//                rs.getBoolean("NULLABLE");
//                rs.getInt("ORDINAL_POSITION");
//                rs.getInt("KEY_SEQ");  
            }
        });
        return tables;
    }

    public static Tables getTables() {
        return tables;
    }
    
}

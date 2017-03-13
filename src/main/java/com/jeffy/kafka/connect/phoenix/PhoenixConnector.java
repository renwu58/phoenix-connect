/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.kafka.connect.phoenix;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jeffy.kafka.connect.phoenix.PhoenixConnection.ConnectionFactory;

import io.debezium.config.Configuration;

/**
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class PhoenixConnector extends SinkConnector{
	private static final Logger log = LoggerFactory.getLogger(PhoenixConnector.class);
	
	private Map<String, String> props;
	
	@Override
	public String version() {
		return "0.1.0";
	}
	@Override
	public void start(Map<String, String> props) {
		log.info("Start the Phoenix Connector.");
		this.props = props;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return PhoenixConnectorTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		log.info("Setting task configurations for {} workers.", maxTasks);
		final List<Map<String,String>> configs = new ArrayList<>(maxTasks);
		IntStream.range(0,maxTasks).forEach((i)->{configs.add(props);});
		return configs;
	}
	@Override
	public void stop() {
		log.info("Stop the Phoenix Connector.");
		this.props = null;
	}
	@Override
	public ConfigDef config() {
		return RuntimeConf.configDef();
	}
	@Override
	public Config validate(Map<String, String> connectorConfigs){
		//验证配置项
		//主要需要验证Phoenix的连接是否能正常工作
		Configuration config = Configuration.from(props);
		Map<String, ConfigValue> results = config.validate(RuntimeConf.DB_FIELDS);
		//目前Phoenix没有启用用户认证，暂时不管用户名与密码
		ConfigValue jdbcURL = results.get(RuntimeConf.JDBC_URL.name());
		if(jdbcURL.errorMessages().isEmpty()){
			ConnectionFactory factory = PhoenixConnection.BasedConnectionFactory(config.getString(RuntimeConf.JDBC_URL));
			PhoenixConfiguration pconfig = PhoenixConfiguration.adapt(config);
			PhoenixConnection conn = new PhoenixConnection(pconfig, factory);
			try {
				conn.connect();
				//随便执行一条语句，没有报错就测试通过
				conn.execute("select table_name from system.catalog limit 1");
			} catch (SQLException e) {
				log.error("Cannot connect to {}, please check the jdbc url and user is right!",config.getString(RuntimeConf.JDBC_URL),e);
				
			}finally{
				try {
					conn.close();
				} catch (SQLException e) {
					// 忽略此错误
				}
			}
		}else{
			log.error("Phoenix database connection string is empty, please input a valid value!");
			context.raiseError(new Exception("Phoenix database connection string is empty, please input a valid value!"));
		}
		return new Config(new ArrayList<>(results.values()));
	}
	
}

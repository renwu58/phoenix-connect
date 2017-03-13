/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.kafka.connect.phoenix;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jeffy.kafka.connect.phoenix.PhoenixConnection.ConnectionFactory;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.data.SchemaUtil;

/**
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class PhoenixConnectorTask extends SinkTask {
	private static final Logger log = LoggerFactory.getLogger(PhoenixConnectorTask.class);
	
	private Configuration config;
	
	private PhoenixConnection conn;
	
	@Override
	public String version() {
		return "0.1.0";
	}

	@Override
	public void start(Map<String, String> props) {
		//此方法在每次更新配置的时候都会调用
		this.config = Configuration.from(props);
		ConnectionFactory factory = PhoenixConnection.BasedConnectionFactory(config.getString(RuntimeConf.JDBC_URL));
		this.conn = new PhoenixConnection(config, factory);
		try {
			conn.connect();
			conn.begin();
		} catch (SQLException e) {
			log.error("Cannot connect to Phoenix Server {}", config.getString(RuntimeConf.JDBC_URL), e);
		}
	}
	@Override
	public void put(Collection<SinkRecord> records) {
		//首先我们必须清楚，一个Topic是对应一个表，还是一个分区对应一个表
		//在此，我们默认一个Topic对应一个表
		records.stream().forEach((record)->{
			log.info("Handling topic {} partition {} offset {}.",record.topic(),  record.kafkaPartition(),record.kafkaOffset());
			if(record.value() instanceof Struct){
				Struct values = (Struct) record.value();
				String op = values.getString(FieldName.OPERATION);
				if(op == null || "".equals(op))//错误的格式
					log.error("Operation cannot be null or empty!");
				if(Operation.CREATE.code().equalsIgnoreCase(op)){
					//插入数据，则数据保存在after部分
					doUpert(record.topic(),values.getStruct(FieldName.AFTER));
				}else if(Operation.DELETE.code().equalsIgnoreCase(op)){
					//删除数据，数据保存在before部分,如果存在主键，则使用主键部分来做删除
					if(record.keySchema().fields().isEmpty())
						doDelete(record.topic(),values.getStruct(FieldName.BEFORE));
					else
						doDelete(record.topic(), ((Struct)record.key()));
				}else if(Operation.UPDATE.code().equalsIgnoreCase(op)){
					//更新数据，before与after都有
					//如果存在主键，一般就直接使用after的数据
					//否则需要先删除before的数据，在插入after的数据，由于Phoenix必须有主键，所以暂时当作都有主键处理
					doUpert(record.topic(),values.getStruct(FieldName.AFTER));
				}else if(Operation.READ.code().equalsIgnoreCase(op)){
					//初始化数据，保存在after部分
					doUpert(record.topic(),values.getStruct(FieldName.AFTER));
				}
				log.info(SchemaUtil.asDetailedString(values));
			}
			if(log.isDebugEnabled())
				log.debug("Key schema {} key value {} value schema {} value {}",SchemaUtil.asDetailedString(record.keySchema()), record.key(), SchemaUtil.asDetailedString(record.valueSchema()), record.value());
		});
	}

	protected void doUpert(String topicName,final Struct value){
		//此处TopicName格式为： instance.schema.tablename
		//我们只需要schema.tablename
		String tableName = topicName.substring(topicName.indexOf(".") + 1);
		if(log.isDebugEnabled())
			log.debug("Upsert topic {} data to table {}", topicName, tableName);
		StringBuffer upsertBuffer = new StringBuffer("UPSERT INTO ");
		upsertBuffer.append(tableName);
		upsertBuffer.append(value.schema().fields().stream().map(Field::name).collect(Collectors.joining(",","(",")")));
		upsertBuffer.append(" VALUES ");
		upsertBuffer.append(value.schema().fields().stream().map((placeholder)->{return "?";}).collect(Collectors.joining(",", "(", ")")));
		if(log.isDebugEnabled())
			log.debug("Upsert SQL: {}", upsertBuffer.toString());
		try(PreparedStatement stmt = conn.createPreparedStatement(upsertBuffer.toString())){
			AtomicInteger index = new AtomicInteger(1);
			value.schema().fields().forEach((field)->{
				try {
					setParameter(stmt, index.getAndIncrement(), field,value);
				} catch (Exception e) {
					log.error("Cannot set parameter {} index {}", field.name(), index.get(), e);
				}
			});
			int rows = stmt.executeUpdate();
			log.debug("Upsert affects {} rows.", rows);
		}catch(SQLException e){
			log.error("Execute upsert statemenet failure!", e);
		}
	}
	protected void doDelete(String topicName, Struct value){
		//如果存在主键，使用主键来做删除，否则使用全字段匹配删除，主要用于处理MySQL没有主键的表
		String tableName = topicName.substring(topicName.indexOf(".") + 1);
		if(log.isDebugEnabled())
			log.debug("Apply delete operation to table {} by topic {}", tableName, topicName);
		StringBuffer upsertBuffer = new StringBuffer("DELETE FROM ");
		upsertBuffer.append(tableName);
		upsertBuffer.append(" WHERE ");
		upsertBuffer.append(value.schema().fields().stream().map((field)->{ return field.name() + "=?";}).collect(Collectors.joining(" AND ")));
		if(log.isDebugEnabled())
			log.debug("Delete SQL: {}", upsertBuffer.toString());
		try(PreparedStatement stmt = conn.createPreparedStatement(upsertBuffer.toString())){
			AtomicInteger index = new AtomicInteger(1);
			value.schema().fields().forEach((field)->{
				try {
					setParameter(stmt, index.getAndIncrement(), field,value);
				} catch (Exception e) {
					log.error("Cannot set parameter {} index {}", field.name(), index.get(), e);
				}
			});
			int rows = stmt.executeUpdate();
			log.debug("Delete affects {} rows.", rows);
		}catch(SQLException e){
			log.error("Execute delete statemenet failure!", e);
		}
	}
	//INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT;
	protected void setParameter(PreparedStatement stmt , int index,Field field, Struct value) throws SQLException{
		switch(field.schema().type()){
			case STRING:
				stmt.setString(index, value.getString(field.name()));
				break;
			case INT32:
				stmt.setInt(index,value.getInt32(field.name()));
				break;
			case INT16:
				stmt.setInt(index,value.getInt16(field.name()));
				break;
			case INT64:
				stmt.setLong(index,value.getInt64(field.name()));
				break;
			case INT8:
				stmt.setInt(index,value.getInt8(field.name()));
				break;
			case FLOAT32:
				stmt.setFloat(index,value.getFloat32(field.name()));
				break;
			case FLOAT64:
				stmt.setDouble(index,value.getFloat64(field.name()));
				break;
			case BOOLEAN:
				stmt.setBoolean(index, value.getBoolean(field.name()));
				break;
			case BYTES:
				stmt.setBytes(index, value.getBytes(field.name()));
				break;
			case ARRAY:
				stmt.setArray(index, new PhoenixArray(PDataType.fromTypeId(Types.ARRAY),value.getArray(field.name()).toArray()));
				break;
			case MAP:
				//Not support
				log.warn("Found not support type {} value {} ", Schema.Type.MAP.getName(),value.getMap(field.name()));
				break;
			case STRUCT:
				log.warn("Found not support type {} value {} ", Schema.Type.STRUCT.getName(), SchemaUtil.asDetailedString(value.getStruct(field.name())));
				break;
			default:
				//未知格式，目前Kafka不支持的格式，一般不会有，有就出大错了
				log.error("Unknow data type {}", field.schema().type());
		}
	}
	
	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		log.info("Flush topic.");
		try {
			conn.commit();
		} catch (SQLException e) {
			log.error("Cannot submit the transaction.", e);
		}
	}
	@Override
	public void stop() {
		if(conn != null){
			try {
				conn.commit();
				conn.close();
			} catch (SQLException e) {
				log.error("Connection close error.", e);
			}
		}
	}

}

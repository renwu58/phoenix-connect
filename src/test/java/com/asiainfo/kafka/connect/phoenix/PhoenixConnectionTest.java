package com.asiainfo.kafka.connect.phoenix;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.jeffy.kafka.connect.phoenix.PhoenixConfiguration;
import com.jeffy.kafka.connect.phoenix.PhoenixConnection;
import com.jeffy.kafka.connect.phoenix.PhoenixConnection.ConnectionFactory;

import io.debezium.config.Configuration;

public class PhoenixConnectionTest {
	private String url = "jdbc:phoenix:master:2181:/hbase-unsecure";
	private String user = "test";
	private String pwd = "test";
	
	private PhoenixConnection conn;
	
	@Before
	public void init(){
		ConnectionFactory factory = PhoenixConnection.BasedConnectionFactory(url);
        Configuration jdbcConfig = Configuration.empty().edit().with(PhoenixConfiguration.JDBC_URL.name(), url).build();
        this.conn = new PhoenixConnection(jdbcConfig, factory);
	}
	
	@Test
	public void testPrepareQuery() {
		fail("Not yet implemented");
	}
	@Test
	public void testExecuteStringArrayAndQuery() {
		String create = "create table if not exists test.user (id integer primary key, name varchar, age integer)";
		String upsert = "upsert into test.user values(1,'aaaaaa',12)";
		String select = "select id,name,age from test.user where id=1";
		String clean = "drop table if exists test.user";
		try {
			conn.connect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			conn.execute(create,upsert).query(select, (ret)->{
				if(ret.next()){
					assertEquals(1, ret.getInt(1));
					assertEquals("aaaaaa",ret.getString(2));
					assertEquals(12,ret.getInt(3));
				}else{
					fail("Test data upsert failure!");
				}
			}).execute(clean);
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Test
	public void testExecuteOperations() {
		fail("Not yet implemented");
	}

	@Test
	public void testReadAllCatalogNames() {
		try {
			conn.connect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//在Phoenix目前一般来讲Table Catalog会为空
		try {
			conn.readAllCatalogNames().forEach((catalog)->{
				System.out.println("Catalog Name: " + catalog);
			});
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Test
	public void testReadAllTableNames() {
		try {
			conn.connect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		final AtomicBoolean found = new AtomicBoolean(false);
		//在Phoenix目前一般来讲Table Catalog会为空
		try {
			conn.readAllTableNames(new String[]{"SYSTEM TABLE", "TABLE", "VIEW"}).forEach((table)->{
				if("SYSTEM".equalsIgnoreCase(table.schema()) && "CATALOG".equalsIgnoreCase(table.table())){
					found.set(true);
				} 
			});
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(!found.get()){
			fail("Get all tables failure!");
		}
		
	}

	@Test
	public void testReadTableNames() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetTablesString() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetTables() {
		fail("Not yet implemented");
	}
	@After
	public void shutdown(){
		if(conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
}

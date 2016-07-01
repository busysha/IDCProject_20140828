package com.xxo.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;



public class RedisUtil  implements  Constant{
	
	protected static final Log logger = LogFactory.getLog(RedisUtil.class);
	
	private static Properties prop;
	private static int PORT; // 6379 redis port
	private static String HOST; // "10.0.7.239" redis host
	
	private static JedisPoolConfig config = null; // Jedis客户端池配置
	private static JedisPool pool = null; // Jedis客户端池
	private static Jedis j = null;
	
	static{
		prop = ConfigUtils.getConfig(JDBC_CONFIG_PATH);
		HOST = prop.getProperty("redis.host", EMPTY);
		PORT = Integer.parseInt(prop.getProperty("redis.port", EMPTY));
		
		config = new JedisPoolConfig();
		config.setMaxActive(300);
		config.setMaxIdle(100);
		config.setMaxWait(10000);
		config.setTestOnBorrow(true);
		logger.debug(PORT + "===-------------------- redis server PORT ");
		logger.debug(HOST + "===-------------------- redis server ip ");
		//线程数量限制，IP地址，端口，超时时间
		pool = new JedisPool(config, HOST, PORT, 60*1000);
	}
	
	//#################   对Hash操作的命令    #####################
	
	/**
	 * 将map中的数据存入到redis
	 * @param tableName  表名
	 * @param totalMap     数据MAP
	 * @param delData        是否删除表中原有的数据
	 */
	public static void putMapData2Redis(String tableName, Map<String, String> totalMap, boolean delData) {
		j = pool.getResource();
		if(delData){
			j.del(tableName); // 删除原始数据
		}
		Pipeline pipe = j.pipelined();
		j.hmset(tableName, totalMap);
		pipe.sync();
		pool.returnResource(j);
	}
	
	/**
	 * 向名称为key的hash中添加元素field
	 * @param key
	 * @param field
	 * @param value
	 */
	public static void putMapData2Redis(String key, String field, String value) {
		j = pool.getResource();
		Pipeline pipe = j.pipelined();
		j.hset(key, field, value);
		pipe.sync();
		pool.returnResource(j);
	}
	
	/**
	 * 根据tableName返回对应的map
	 * 
	 * @param tableName
	 * @return
	 */
	public static Map<String, String> findCode2CodeIDMap(String tableName) {
		j = pool.getResource();
		Map<String, String> rsMap = j.hgetAll(tableName);
		pool.returnResource(j);
		return null != rsMap ? rsMap : new HashMap<String, String>();
	}
	
	/**
	 * 根据key field取值
	 * 返回名称为key的hash中field对应的value
	 * @param key
	 * @param field
	 * @return
	 */
	public static String findValue(String key, String field) {
		j = pool.getResource();
		String value = j.hget(key, field);
		pool.returnResource(j);
		return value;
	}
	
	
	
	//#################   对String操作的命令    #####################
	
	/**
	 * 
	 * @param key
	 * @param value
	 */
	public static void putStringData2Redis(String key, String value) {
		j = pool.getResource();
		Pipeline pipe = j.pipelined();
		j.set(key, value);
		pipe.sync();
		pool.returnResource(j);
	}

	public static String getStringData2Redis(String key) {
		j = pool.getResource();
		Pipeline pipe = j.pipelined();
		String s = j.get(key);
		pipe.sync();
		pool.returnResource(j);
		return s;
	}
	
	/**
	 * 根据key删除表
	 * 
	 * @param key
	 */
	public static void del(String... key) {
		j = pool.getResource();
		System.out.println("删除redis：" + key);
		j.del(key);
		pool.returnResource(j);
	}


	/**
	 * 关闭redis连接
	 */
	public static void closeRedis() {
		if (j != null) {
			j.disconnect();
		}
		if (pool != null) {
			pool.destroy();
		}
		config = null;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<String, String> house_ip_info = findCode2CodeIDMap("house_ip_info");
		for (Map.Entry<String, String> entry : house_ip_info.entrySet()) {
			System.out.println( entry.getKey() + "	" + entry.getValue() );
		}
	}

}

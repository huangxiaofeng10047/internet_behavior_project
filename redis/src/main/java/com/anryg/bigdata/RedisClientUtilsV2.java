package com.anryg.bigdata;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;


public class RedisClientUtilsV2 implements RedisParam {
    private static volatile JedisPool jedisPool = null;
    /**
     * 用连接池进行管理
     */
    private static Jedis jedis = null;
    private static final ThreadLocal<Jedis> threadLocal = new ThreadLocal<>();

    /**
     * @DESC: 初始化连接池
     **/
    private static void initPool() {
        JedisPoolConfig config = null;
        try {
            config = new JedisPoolConfig();
            config.setMaxTotal(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
            //使用时进行扫描，确保都可用
            config.setTestWhileIdle(true);
            //Idle时进行连接扫描
            config.setTestOnReturn(true);//还回线程池时进行扫描
        } catch (Exception e) {
            throw e;
        }
        jedisPool = new JedisPool(config, HOSTS.split(",")[0], PORT, TIMEOUT, PASSWD);
    }

    /**
     * @DESC: 多线程环境下确保只初始化一个连接池
     */
    private static void poolInit() {
        if (jedisPool == null) {
            synchronized (RedisClientUtilsV2.class) {
                if (jedisPool == null) initPool();
            }
        }
    }

    /**
     * @DESC: 获取连接池对象，适用多线程时，利用其获取多个jedis客户端
     **/
    public static JedisPool getJedisPool() {
        poolInit();
        return jedisPool;
    }

    /**
     * @DESC: 同步获取JedisdThreadLocal实例
     * @return ThreadLocal<Jedis>
     */
    public static ThreadLocal<Jedis> getRedisThreadLocal() {
        poolInit();
        if (threadLocal.get() == null) {
            synchronized (RedisClientUtilsV2.class) {
                if (threadLocal.get() == null) {
                    jedis = jedisPool.getResource();
                    threadLocal.set(jedis);
                }
            }
        }
        return threadLocal;
    }

    /**
     * @DESC: 释放jedis资源,将资源放回连接池
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null && jedisPool != null) jedis.close();
    }
    public static Set<String> getSetResult(Jedis redis, int redisNo, String key){
        redis.select(redisNo);
        Set<String> scanResult = null;
        try {
            scanResult = redis.smembers(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redis.close();
        }
        return scanResult;
    }
}

package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.jupiter.api.Test;
import sunyu.util.RedisUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestRedisUtil {
    Log log = LogFactory.get();

    @Test
    void t001() {
        // Syntax: redis://[password@]host[:port][/databaseNumber]
        // Syntax: redis://[username:password@]host[:port][/databaseNumber]
        RedisClient redisClient = RedisClient.create("redis://192.168.11.39:16379/0");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        log.debug("Connected to Redis");
        RedisCommands<String, String> redisCommands = connection.sync();

        log.debug(redisCommands.get("subsidy:bc:userinfo"));

        connection.close();
        redisClient.shutdown();
    }

    @Test
    void t002() {
        // Syntax: redis://[password@]host[:port]
        // Syntax: redis://[username:password@]host[:port]
        List<RedisURI> uris = new ArrayList();
        uris.add(RedisURI.create("redis://192.168.11.124:7001"));
        uris.add(RedisURI.create("redis://192.168.11.124:7002"));
        uris.add(RedisURI.create("redis://192.168.11.124:7003"));
        uris.add(RedisURI.create("redis://192.168.11.125:7004"));
        uris.add(RedisURI.create("redis://192.168.11.125:7005"));
        uris.add(RedisURI.create("redis://192.168.11.125:7006"));
        RedisClusterClient redisClient = RedisClusterClient.create(uris);
        StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
        log.debug("Connected to Redis Cluster");
        RedisAdvancedClusterCommands<String, String> redisClusterCommands = connection.sync();

        for (KeyValue<String, String> kv : redisClusterCommands.mget("relation:16200442", "farm:realtime:0865306056453850")) {
            log.debug("{} {}", kv.getKey(), kv.getValue());
        }

        connection.close();
        redisClient.shutdown();
    }

    @Test
    void t003() {
        // Syntax: redis-sentinel://[password@]host[:port][,host2[:port2]][/databaseNumber]#sentinelMasterId
        RedisClient redisClient = RedisClient.create("redis-sentinel://localhost:26379,localhost:26380/0#mymaster");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        log.debug("Connected to Redis using Redis Sentinel");
        RedisCommands<String, String> redisSentinelCommands = connection.sync();

        connection.close();
        redisClient.shutdown();
    }


    @Test
    void t004() {
        RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
        RedisCommands<String, String> standalone = redisUtil.standalone("redis://192.168.11.39:16379/0");//全局只需要一个

        log.debug(standalone.get("subsidy:bc:userinfo"));

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }

    @Test
    void t005() {
        RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
        RedisAdvancedClusterCommands<String, String> cluster = redisUtil.cluster(
                Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                        "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

        for (KeyValue<String, String> kv : cluster.mget("relation:16200442", "farm:realtime:0865306056453850")) {
            log.debug("{} {}", kv.getKey(), kv.getValue());
        }

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }

}

package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
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
import java.util.concurrent.ExecutionException;

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
        StatefulRedisConnection<String, String> standalone = redisUtil.standalone("redis://192.168.11.39:16379/0");//全局只需要一个

        //同步调用
        log.debug(standalone.sync().get("subsidy:bc:userinfo"));

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }

    @Test
    void t005() {
        RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
        StatefulRedisClusterConnection<String, String> cluster = redisUtil.cluster(
                Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                        "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

        //同步调用
        for (KeyValue<String, String> kv : cluster.sync().mget("relation:16200442", "farm:realtime:0865306056453850")) {
            if (kv.isEmpty()) {
                log.debug("{}", kv);
            } else {
                log.debug("{} {}", kv.getKey(), kv.getValue());
            }
        }

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }

    @Test
    void t006() {
        RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
        StatefulRedisClusterConnection<String, String> cluster = redisUtil.cluster(
                Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                        "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

        for (KeyValue<String, String> kv : cluster.sync().mget("farm:realtime:TEST202406259541", "farm:realtime:EM9101B1DYCFESHJ", "farm:realtime:XJXY142230900083", "farm:realtime:TEST202406259660", "farm:realtime:TEST202412345600", "farm:realtime:EC73BT2405280002", "farm:realtime:test202406265000", "farm:realtime:TEST102404G002931", "farm:realtime:test202406259846", "farm:realtime:TEST202406259005", "farm:realtime:TESTDNG2023081103", "farm:realtime:TEST202406259624", "farm:realtime:TEST202406259841", "farm:realtime:TEST202407109635", "farm:realtime:TEST202406259220", "farm:realtime:TEST202406256565", "farm:realtime:TEST202406279850", "farm:realtime:TEST202406279856", "farm:realtime:TEST202407049036", "farm:realtime:TEST202406256987", "farm:realtime:TEST202406279652", "farm:realtime:TEST2024062596536", "farm:realtime:TEST2024062795632", "farm:realtime:TEST202406251118", "farm:realtime:TESTDNG2024062994", "farm:realtime:TESTDNG2024062993", "farm:realtime:TEST202406259006", "farm:realtime:test5678901234590", "farm:realtime:BCTEST2109100015", "farm:realtime:TEST202406254447", "farm:realtime:TEST202406259964", "farm:realtime:TEST202406259000", "farm:realtime:TEST202406259004", "farm:realtime:33333333333333333333333333333333", "farm:realtime:TEST202406269562", "farm:realtime:EC1234567812312343", "farm:realtime:test202406265666", "farm:realtime:TEST202406259611", "farm:realtime:TEST2024062596310", "farm:realtime:TEST202406259998", "farm:realtime:TEST202406259357", "farm:realtime:TEST2024062596358", "farm:realtime:test2022052598789", "farm:realtime:TEST202406249651", "farm:realtime:SUNYUDIDTEST00002", "farm:realtime:600064", "farm:realtime:GUOTEST000000001", "farm:realtime:test202405286953", "farm:realtime:TESTDNG2023081104", "farm:realtime:EC71BT2405110185")) {
            if (kv.isEmpty()) {
                log.debug("{}", kv);
            } else {
                log.debug("{} {}", kv.getKey(), kv.getValue());
            }
        }

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }

    @Test
    void t007() {
        RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
        StatefulRedisConnection<String, String> standalone = redisUtil.standalone("redis://192.168.11.39:16379/0");//全局只需要一个

        //异步调用
        RedisFuture<String> stringRedisFuture = standalone.async().get("subsidy:bc:userinfo");
        try {
            String v = stringRedisFuture.get();//等待异步方法返回
            log.info("{}", v);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }

    @Test
    void t008() {
        RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
        StatefulRedisClusterConnection<String, String> cluster = redisUtil.cluster(
                Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                        "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

        //异步调用
        RedisFuture<List<KeyValue<String, String>>> mget = cluster.async().mget("relation:16200442", "farm:realtime:0865306056453850");
        try {
            for (KeyValue<String, String> kv : mget.get()) {
                if (kv.isEmpty()) {
                    log.debug("{}", kv);
                } else {
                    log.debug("{} {}", kv.getKey(), kv.getValue());
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        redisUtil.close();//如果程序不再使用了，可以调用这个
    }
}

package sunyu.util.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.jupiter.api.Test;
import sunyu.util.RedisClusterUtil;
import sunyu.util.RedisUtil;
import sunyu.util.test.config.ConfigProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestRedisUtil {
    Log log = LogFactory.get();

    private static final Props props = ConfigProperties.getProps();

    /**
     * 单节点模式
     */
    @Test
    void testStandalone() {
        // Syntax: redis://[password@]host[:port][/databaseNumber]
        // Syntax: redis://[username:password@]host[:port][/databaseNumber]
        //RedisClient redisClient = RedisClient.create("redis://192.168.11.39:16379/0");
        RedisClient redisClient = RedisClient.create("redis://172.16.1.22:26379/0");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        log.debug("Connected to Redis");
        RedisCommands<String, String> redisCommands = connection.sync();

        // todo
        log.debug(redisCommands.get("AgFarm-1525b3d086d9439c99cab5acfd565532-15-800-600"));

        connection.close();
        redisClient.shutdown();
    }

    /**
     * 集群模式
     */
    @Test
    void testCluster() {
        // Syntax: redis://[password@]host[:port]
        // Syntax: redis://[username:password@]host[:port]
        List<RedisURI> uris = new ArrayList<>();
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

        // todo
        for (KeyValue<String, String> kv : redisClusterCommands.mget("relation:16200442",
                "farm:realtime:0865306056453850")) {
            log.debug("{} {}", kv.getKey(), kv.getValue());
        }

        connection.close();
        redisClient.shutdown();
    }

    /**
     * 哨兵模式
     */
    @Test
    void testSentinel() {
        // Syntax: redis-sentinel://[password@]host[:port][,host2[:port2]][/databaseNumber]#sentinelMasterId
        RedisClient redisClient = RedisClient.create("redis-sentinel://localhost:26379,localhost:26380/0#mymaster");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        log.debug("Connected to Redis using Redis Sentinel");
        RedisCommands<String, String> redisSentinelCommands = connection.sync();

        // todo
        String v = redisSentinelCommands.get("key");
        log.info(v);

        connection.close();
        redisClient.shutdown();
    }

    @Test
    void testGet() {
        RedisUtil redisStandaloneUtil = RedisUtil.builder().uri("redis://192.168.11.39:16379/0")
                .build();
        String v = redisStandaloneUtil.getCommands().get("subsidy:bc:userinfo");
        log.info(v);
        v = redisStandaloneUtil.get("subsidy:bc:userinfo");
        log.info(v);
        redisStandaloneUtil.close();

        /*RedisSentinelUtil redisSentinelUtil = RedisSentinelUtil.builder().uri("redis-sentinel://localhost:26379,localhost:26380/0#mymaster").build();
        v = redisSentinelUtil.getCommands().get("subsidy:bc:userinfo");
        log.info(v);
        v = redisSentinelUtil.get("subsidy:bc:userinfo");
        log.info(v);
        redisSentinelUtil.close();*/
    }

    @Test
    void testMget() {
        RedisClusterUtil redisClusterUtil = RedisClusterUtil.builder()
                .nodes("192.168.11.124:7001,192.168.11.124:7002,192.168.11.124:7003,192.168.11.125:7004,192.168.11.125:7005,192.168.11.125:7006")
                .build();

        for (KeyValue<String, String> kv : redisClusterUtil.getCommands().mget("relation:16200442",
                "farm:realtime:0865306056453850", "abc")) {
            if (kv.isEmpty()) {
                log.debug("{}", kv.getKey());
            } else {
                log.debug("{} {}", kv.getKey(), kv.getValue());
            }
        }

        redisClusterUtil.close();
    }

    @Test
    void testGet2() {
        RedisClusterUtil redisClusterUtil = RedisClusterUtil.builder()
                .nodes("192.168.11.124:7001,192.168.11.124:7002,192.168.11.124:7003,192.168.11.125:7004,192.168.11.125:7005,192.168.11.125:7006")
                .build();

        for (int i = 0; i < 20; i++) {
            String v = redisClusterUtil.get("relation:16200442");
            log.info(v);
            ThreadUtil.sleep(1000 * 10);
        }

        redisClusterUtil.close();
    }

    @Test
    void testScan() {
        RedisClusterUtil redisClusterUtil = RedisClusterUtil.builder()
                .uriStrList(Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002",
                        "redis://192.168.11.124:7003",
                        "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"))
                .build();

        redisClusterUtil.scan("ne:realtime:*", 500, key -> {
            log.info("{}", key);
        });

        redisClusterUtil.close();
    }

    @Test
    void testGeoradius() {
        RedisUtil redisStandaloneUtil = RedisUtil.builder()
                .uri("redis://11KYms98qm@192.168.13.73:30794/2")
                .build();

        // GEOADD places {longitude} {latitude} "{address_name}"
        redisStandaloneUtil.geoadd("testgeo", 116.37304, 39.92594, "北京市西城区什刹海街道西什库大街19号院");

        // GEORADIUS places {longitude} {latitude} {radius} m COUNT 1 ASC
        String member = redisStandaloneUtil.georadiusWithCountOne("testgeo", 116.37304, 39.92594, 1);
        log.info(member);

        redisStandaloneUtil.close();
    }

    @Test
    void testZset() {
        RedisUtil redisStandaloneUtil = RedisUtil.builder()
                .uri("redis://rPo4IdPmg9@172.16.1.180:31091/2")
                .build();

        RedisCommands<String, String> command = redisStandaloneUtil.getCommands();

        // 将zset中所有数据分批取出来复制到另一个zset中，避免一次性取出大量数据导致内存问题
        String sourceKey = "pca";
        String targetKey = "pac:baidu";
        int batchSize = 1000; // 每批处理1000条数据
        long offset = 0;

        while (true) {
            // 分批获取数据
            List<io.lettuce.core.ScoredValue<String>> batch = command.zrangeWithScores(sourceKey, offset,
                    offset + batchSize - 1);

            if (batch.isEmpty()) {
                break; // 没有更多数据了
            }

            // 将这批数据添加到目标zset中
            for (io.lettuce.core.ScoredValue<String> member : batch) {
                log.debug("{} {}", member.getScore(), member.getValue());
                //command.zadd(targetKey, member.getScore(), member.getValue());
            }

            // 如果这批数据不足batchSize，说明已经处理完了
            if (batch.size() < batchSize) {
                break;
            }

            offset += batchSize;
        }

        redisStandaloneUtil.close();
    }

}

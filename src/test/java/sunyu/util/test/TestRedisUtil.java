package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import io.lettuce.core.KeyValue;
import org.junit.jupiter.api.Test;
import sunyu.util.RedisClusterUtil;
import sunyu.util.RedisUtil;
import sunyu.util.test.config.ConfigProperties;

import java.util.List;
import java.util.Map;

public class TestRedisUtil {
    Log log = LogFactory.get();

    private static final Props props = ConfigProperties.getProps();

    @Test
    void testCluster() {
        RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
                .nodes(props.getStr("redis.cluster.nodes"))
                .build();

        String v = clusterUtil.get("p:r:d:600243");
        log.info("{}", v);

        clusterUtil.close();
    }

    @Test
    void testScan() {
        RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
                .nodes(props.getStr("redis.cluster.nodes"))
                .build();

        clusterUtil.scan("p:r:v:*", 500, key -> log.info("{}", key));

        clusterUtil.close();
    }

    @Test
    void testMget() {
        RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
                .nodes(props.getStr("redis.cluster.nodes"))
                .build();

        Map<String, String> kvs = clusterUtil.mget("farm:realtime:600044", "abc", "farm:realtime:600179");
        kvs.forEach((k, v) -> log.info("{} {}", k, v));

        clusterUtil.close();
    }

    @Test
    void testMget2() {
        RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
                .nodes(props.getStr("redis.cluster.nodes"))
                .build();

        List<KeyValue<String, String>> lvs = clusterUtil.getCommands().mget("farm:realtime:600044", "abc", "farm:realtime:600179");
        // 检查lvs是否为null
        if (lvs != null) {
            for (KeyValue<String, String> kv : lvs) {
                // 旧版Lettuce没有isEmpty方法，需要手动检查KeyValue是否有效
                // 通过检查hasValue()和getValue()是否为null来判断
                if (kv != null && kv.hasValue() && kv.getValue() != null) {
                    // 安全日志输出，避免空指针
                    log.info("{} {}", kv.getKey() != null ? kv.getKey() : "null", kv.getValue());
                } else {
                    // KeyValue无效或值为null的情况
                    if (kv != null) {
                        log.info("{} null", kv.getKey() != null ? kv.getKey() : "null");
                    } else {
                        log.info("KeyValue is null");
                    }
                }
            }
        } else {
            log.info("MGET returned null");
        }

        clusterUtil.close();
    }

    @Test
    void testStandalone() {
        RedisUtil standaloneUtil = new RedisUtil.Builder()
                .uri(props.getStr("redis.standalone.uri"))
                .build();
        String v = standaloneUtil.georadiusWithCountOne("pca:tianditu", 86.018138, 28.283572, 1000);
        log.info("{}", v);

        standaloneUtil.close();
    }
}
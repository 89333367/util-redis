package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import org.junit.jupiter.api.Test;
import sunyu.util.RedisClusterUtil;
import sunyu.util.RedisUtil;
import sunyu.util.test.config.ConfigProperties;

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
    void testStandalone() {
        RedisUtil standaloneUtil = new RedisUtil.Builder()
                .uri(props.getStr("redis.standalone.uri"))
                .build();
        String v = standaloneUtil.georadiusWithCountOne("pca:tianditu", 86.018138, 28.283572, 1000);
        log.info("{}", v);

        standaloneUtil.close();
    }
}

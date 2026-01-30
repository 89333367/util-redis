package sunyu.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.*;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis 集群工具类
 *
 * @author SunYu
 */
public class RedisClusterUtil extends AbstractRedisOperations<RedisAdvancedClusterCommands<String, String>>
        implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisClusterUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());
        config.client = RedisClusterClient.create(config.uriList);

        log.info("构建集群拓扑刷新策略");
        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                // 周期性刷新（延长到10秒，减少集群压力）
                .enablePeriodicRefresh(Duration.ofSeconds(10))
                // 显式启用自适应刷新
                .enableAllAdaptiveRefreshTriggers()
                // 限制刷新频率
                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(30))  // 延长至30秒
                .build();

        log.info("构建集群客户端选项");
        ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                .topologyRefreshOptions(topologyRefreshOptions)
                // 命令超时（保持不变）
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(30)))
                // Socket 配置（保持不变）
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .keepAlive(true)
                        .tcpNoDelay(true)
                        .build())
                // 断开行为（保持不变）
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build();

        config.client.setOptions(clusterClientOptions);

        log.info("建立集群连接");
        config.connection = config.client.connect();

        log.info("设置读取策略: {}", ReadFrom.REPLICA_PREFERRED);
        config.connection.setReadFrom(ReadFrom.REPLICA_PREFERRED);

        config.commands = config.connection.sync();
        log.info("[构建 {}] 结束", this.getClass().getSimpleName());

        this.config = config;
    }

    private static class Config {
        private List<RedisURI> uriList;
        private RedisClusterClient client;
        private StatefulRedisClusterConnection<String, String> connection;
        private RedisAdvancedClusterCommands<String, String> commands;
    }

    public static class Builder {
        private final Config config = new Config();

        public RedisClusterUtil build() {
            return new RedisClusterUtil(config);
        }

        /**
         * 设置连接
         *
         * @param nodes 192.168.11.124:7001,192.168.11.124:7002,192.168.11.124:7003,192.168.11.125:7004,192.168.11.125:7005,192.168.11.125:7006
         * @return
         */
        public Builder nodes(String nodes) {
            List<String> l = Arrays.stream(nodes.split(","))
                    .map(s -> s.split(":"))
                    .map(arr -> StrUtil.format("redis://{}:{}", arr[0], arr[1]))
                    .collect(Collectors.toList());
            return uriStrList(l);
        }

        /**
         * 设置连接
         * <pre>
         * redis://[password@]host[:port]
         * redis://[username:password@]host[:port]
         * </pre>
         *
         * @param uriStrList
         * @return
         */
        public Builder uriStrList(List<String> uriStrList) {
            List<RedisURI> l = new ArrayList<>();
            for (String uri : uriStrList) {
                l.add(RedisURI.create(uri));
            }
            return uriList(l);
        }

        /**
         * 设置连接
         *
         * @param uriList
         * @return
         */
        public Builder uriList(List<RedisURI> uriList) {
            config.uriList = uriList;
            return this;
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("[销毁 {}] 开始", this.getClass().getSimpleName());
        config.connection.close();
        config.client.shutdown();
        log.info("[销毁 {}] 开始", this.getClass().getSimpleName());
    }

    /**
     * 获取同步命令对象
     *
     * @return
     */
    public RedisAdvancedClusterCommands<String, String> getCommands() {
        return config.commands;
    }

}
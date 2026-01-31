package sunyu.util;

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
import java.util.List;

/**
 * Redis 集群工具类
 *
 * @author SunYu
 */
public class RedisClusterUtil
        extends AbstractRedisOperations<String, String, RedisAdvancedClusterCommands<String, String>>
        implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisClusterUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());
        // 分割逗号分隔的 URI 字符串，转换为 RedisURI 列表
        List<RedisURI> redisURIs = new ArrayList<>();
        for (String uri : config.uri.split(",")) {
            redisURIs.add(RedisURI.create(uri.trim()));
        }
        log.info("Redis 集群节点: {}", redisURIs);
        config.client = RedisClusterClient.create(redisURIs);

        log.info("构建集群拓扑刷新策略");
        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                // 周期性刷新（延长到10秒，减少集群压力）
                .enablePeriodicRefresh(Duration.ofSeconds(10))
                // 显式启用自适应刷新
                .enableAllAdaptiveRefreshTriggers()
                // 限制刷新频率
                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(30)) // 延长至30秒
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
        private String uri;
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
         * 设置链接
         *
         * <pre>
         *     redis :// [[username :] password@] host [:port][/database]
         *           [?[timeout=timeout[d|h|m|s|ms|us|ns]] [&clientName=clientName]
         *           [&libraryName=libraryName] [&libraryVersion=libraryVersion] ]
         * </pre>
         *
         * <pre>
         *     rediss :// [[username :] password@] host [: port][/database]
         *            [?[timeout=timeout[d|h|m|s|ms|us|ns]] [&clientName=clientName]
         *            [&libraryName=libraryName] [&libraryVersion=libraryVersion] ]
         * </pre>
         *
         * <pre>
         *     redis-socket :// [[username :] password@]path
         *                  [?[timeout=timeout[d|h|m|s|ms|us|ns]] [&database=database]
         *                  [&clientName=clientName] [&libraryName=libraryName]
         *                  [&libraryVersion=libraryVersion] ]
         * </pre>
         *
         * <pre>
         *      redis://192.168.11.124:7001,redis://192.168.11.124:7002,redis://192.168.11.124:7003,redis://192.168.11.125:7004,redis://192.168.11.125:7005,redis://192.168.11.125:7006
         * </pre>
         *
         * @param uri 链接
         * @return 构建器
         */
        public Builder uri(String uri) {
            config.uri = uri;
            return this;
        }

        /**
         * 设置节点
         *
         * <pre>
         *      192.168.11.124:7001,192.168.11.124:7002,192.168.11.124:7003,192.168.11.125:7004,192.168.11.125:7005,192.168.11.125:7006
         * </pre>
         *
         * @param nodes 节点
         * @return 构建器
         */
        public Builder nodes(String nodes) {
            StringBuilder uriBuilder = new StringBuilder();
            for (String node : nodes.split(",")) {
                uriBuilder.append("redis://").append(node.trim()).append(",");
            }
            config.uri = uriBuilder.substring(0, uriBuilder.length() - 1);
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
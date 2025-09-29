package sunyu.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RedisClusterUtil extends AbstractRedisOperations<RedisAdvancedClusterCommands<String, String>> implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisClusterUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());
        config.client = RedisClusterClient.create(config.uriList);
        log.info("构建集群拓扑参数开始");
        ClusterTopologyRefreshOptions clusterTopologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh()//周期性更新集群拓扑视图
                .enableAllAdaptiveRefreshTriggers()//设置自适应更新集群拓扑视图触发器
                .build();
        log.info("构建集群拓扑参数完毕");
        log.info("构建集群客户端参数开始");
        ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                .topologyRefreshOptions(clusterTopologyRefreshOptions)
                .build();
        log.info("构建集群客户端参数完毕");
        log.info("设置集群客户端参数开始");
        config.client.setOptions(clusterClientOptions);
        log.info("设置集群客户端参数结束");
        config.connection = config.client.connect();
        log.info("设置读取策略开始");
        config.connection.setReadFrom(ReadFrom.REPLICA_PREFERRED);//设置为从副本中读取首选值，如果没有可用的副本，则回退到上游。
        log.info("设置读取策略完毕 {}", config.connection.getReadFrom());
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
         *
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
         *
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
         *
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
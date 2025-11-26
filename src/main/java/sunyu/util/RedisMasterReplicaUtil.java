package sunyu.util;

import java.time.Duration;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.system.SystemUtil;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SocketOptions.KeepAliveOptions;
import io.lettuce.core.SocketOptions.TcpUserTimeoutOptions;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

/**
 * Redis 主从工具类
 * @author SunYu
 */
public class RedisMasterReplicaUtil extends AbstractRedisOperations<RedisCommands<String, String>>
        implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisMasterReplicaUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());
        config.client = RedisClient.create();
        if (!SystemUtil.getOsInfo().isWindows()) {
            log.info("构建KeepAlive参数开始");
            SocketOptions socketOptions = SocketOptions.builder()
                    .keepAlive(KeepAliveOptions.builder()
                            .enable()
                            .idle(Duration.ofSeconds(config.TCP_KEEPALIVE_IDLE))
                            .interval(Duration.ofSeconds(config.TCP_KEEPALIVE_IDLE / 3))
                            .count(3)
                            .build())
                    .tcpUserTimeout(TcpUserTimeoutOptions.builder()
                            .enable()
                            .tcpUserTimeout(Duration.ofSeconds(config.TCP_USER_TIMEOUT))
                            .build())
                    .build();
            log.info("构建KeepAlive参数完毕");
            config.client.setOptions(ClientOptions.builder()
                    .socketOptions(socketOptions)
                    .build());
        }
        config.connection = MasterReplica.connect(config.client, StringCodec.UTF8, RedisURI.create(config.uri));
        config.connection.setReadFrom(ReadFrom.REPLICA_PREFERRED);//设置为从副本中读取首选值，如果没有可用的副本，则回退到上游。
        config.commands = config.connection.sync();
        log.info("[构建 {}] 结束", this.getClass().getSimpleName());

        this.config = config;
    }

    private static class Config {
        private RedisClient client;
        private String uri;
        private StatefulRedisMasterReplicaConnection<String, String> connection;
        private RedisCommands<String, String> commands;
        /**
        *  TCP_KEEPALIVE打开，并且配置三个参数分别为:
        *  TCP_KEEPIDLE = 30
        *  TCP_KEEPINTVL = 10
        *  TCP_KEEPCNT = 3
        */
        private final int TCP_KEEPALIVE_IDLE = 30;

        /**
         * TCP_USER_TIMEOUT参数可以避免在故障宕机场景下，Lettuce持续超时的问题。
         * refer: https://github.com/lettuce-io/lettuce-core/issues/2082
         */
        private final int TCP_USER_TIMEOUT = 30;
    }

    public static class Builder {
        private final Config config = new Config();

        public RedisMasterReplicaUtil build() {
            return new RedisMasterReplicaUtil(config);
        }

        /**
         * 链接
         * <pre>
         * redis-sentinel://localhost:26379,localhost:26380/0#mymaster
         * redis-sentinel://[password@]host[:port][,host2[:port2]][/databaseNumber]#sentinelMasterId
         * </pre>
         *
         * @param uri
         *
         * @return
         */
        public Builder uri(String uri) {
            config.uri = uri;
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
        log.info("[销毁 {}] 结束", this.getClass().getSimpleName());
    }

    /**
     * 获取同步命令对象
     *
     * @return
     */
    public RedisCommands<String, String> getCommands() {
        return config.commands;
    }

}
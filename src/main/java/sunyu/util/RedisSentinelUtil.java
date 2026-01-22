package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.*;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

import java.time.Duration;

/**
 * Redis 哨兵工具类
 *
 * @author SunYu
 */
public class RedisSentinelUtil extends AbstractRedisOperations<RedisCommands<String, String>>
        implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisSentinelUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());

        // 1. 创建客户端
        config.client = RedisClient.create();

        // 2. 配置客户端选项（必须：超时 + 保活）
        ClientOptions clientOptions = ClientOptions.builder()
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(30)))
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .keepAlive(true)
                        .tcpNoDelay(true)
                        .build())
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build();
        config.client.setOptions(clientOptions);

        // 3. 哨兵 URI（标准格式）
        //    注意：如果要密码认证，放在最前面
        RedisURI sentinelUri = RedisURI.create(config.uri);

        // 4. 建立连接（自动发现主节点）
        config.connection = MasterReplica.connect(
                config.client,
                StringCodec.UTF8,
                sentinelUri
        );

        // 5. 设置读取策略
        config.connection.setReadFrom(ReadFrom.REPLICA_PREFERRED);

        // 6. 创建命令接口
        config.commands = config.connection.sync();

        log.info("[构建 {}] 结束", this.getClass().getSimpleName());

        this.config = config;
    }

    private static class Config {
        private RedisClient client;
        private String uri;
        private StatefulRedisMasterReplicaConnection<String, String> connection;
        private RedisCommands<String, String> commands;
    }

    public static class Builder {
        private final Config config = new Config();

        public RedisSentinelUtil build() {
            return new RedisSentinelUtil(config);
        }

        /**
         * 链接
         * <pre>
         * redis-sentinel://:yourPassword@192.168.1.101:26379,192.168.1.102:26379,192.168.1.103:26379/0#mymaster
         * </pre>
         *
         * @param uri
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
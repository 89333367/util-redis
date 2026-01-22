package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.*;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis 主从工具类
 *
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

        // 1. 创建客户端（必须配置超时）
        config.client = RedisClient.create();

        // 2. 配置客户端选项（关键：超时）
        ClientOptions clientOptions = ClientOptions.builder()
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(30)))  // 命令超时30秒
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(5))   // 连接超时5秒
                        .keepAlive(true)                        // 启用TCP KeepAlive
                        .tcpNoDelay(true)
                        .build())
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build();
        config.client.setOptions(clientOptions);

        // 3. 提供主从所有节点地址（静态配置）
        //    格式：redis://master-host:6379,redis://slave1-host:6379,redis://slave2-host:6379
        List<RedisURI> redisUris = new ArrayList<>();
        for (String s : config.uri.split(",")) {
            redisUris.add(RedisURI.create(s));
        }

        // 4. 建立主从连接
        config.connection = MasterReplica.connect(
                config.client,
                StringCodec.UTF8,
                redisUris  // 传入所有节点URI，Lettuce自动识别主从角色
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

        public RedisMasterReplicaUtil build() {
            return new RedisMasterReplicaUtil(config);
        }

        /**
         * 链接
         * <pre>
         * redis://master-host:6379,redis://slave1-host:6379,redis://slave2-host:6379
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
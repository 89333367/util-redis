package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Redis 单机、哨兵工具类
 *
 * @author SunYu
 */
public class RedisUtil extends AbstractRedisOperations<RedisCommands<String, String>>
        implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());
        config.client = RedisClient.create(config.uri);
        config.connection = config.client.connect();
        config.commands = config.connection.sync();
        log.info("[构建 {}] 结束", this.getClass().getSimpleName());

        this.config = config;
    }

    private static class Config {
        private RedisClient client;
        private String uri;
        private StatefulRedisConnection<String, String> connection;
        private RedisCommands<String, String> commands;
    }

    public static class Builder {
        private final Config config = new Config();

        public RedisUtil build() {
            return new RedisUtil(config);
        }

        /**
         * 链接，支持单机和哨兵模式
         * <pre>
         * redis://localhost:16379/0
         * redis://[password@]host[:port][/databaseNumber]
         * redis://[username:password@]host[:port][/databaseNumber]
         * rediss://[[username:]password@]host[:port][/database][?[timeout=timeout[d|h|m|s|ms|us|ns]][&clientName=clientName][&libraryName=libraryName][&libraryVersion=libraryVersion]]
         * redis-socket://[[username:]password@]path[?[timeout=timeout[d|h|m|s|ms|us|ns]][&database=database][&clientName=clientName][&libraryName=libraryName][&libraryVersion=libraryVersion]]
         * redis-sentinel://password@sentinel1:26379?sentinelMasterId=master
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
package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

public class RedisSentinelUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RedisSentinelUtil(Config config) {
        log.info("[构建RedisSentinelUtil] 开始");
        config.client = RedisClient.create();
        config.connection = MasterReplica.connect(config.client, StringCodec.UTF8, RedisURI.create(config.uri));
        config.connection.setReadFrom(ReadFrom.UPSTREAM_PREFERRED);//设置读取策略
        config.commands = config.connection.sync();
        log.info("[构建RedisSentinelUtil] 结束");

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
         * redis-sentinel://localhost:26379,localhost:26380/0#mymaster
         * redis-sentinel://[password@]host[:port][,host2[:port2]][/databaseNumber]#sentinelMasterId
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
        log.info("[回收RedisSentinelUtil] 开始");
        config.connection.close();
        config.client.shutdown();
        log.info("[回收RedisSentinelUtil] 结束");
    }

    /**
     * 获取同步命令对象
     *
     * @return
     */
    public RedisCommands<String, String> getCommands() {
        return config.commands;
    }

    /**
     * 获取值
     *
     * @param key
     * @return
     */
    public String get(String key) {
        return config.commands.get(key);
    }

}
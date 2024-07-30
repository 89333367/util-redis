package sunyu.util;

import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis工具类
 *
 * @author 孙宇
 */
public enum RedisUtil implements Serializable, Closeable {
    INSTANCE;
    private Log log = LogFactory.get();

    private Map<String, RedisClient> redisClientMap = new HashMap<>();
    private Map<String, RedisClusterClient> redisClusterClientMap = new HashMap<>();
    private Map<String, StatefulRedisConnection<String, String>> redisConnectionMap = new HashMap<>();
    private Map<String, StatefulRedisClusterConnection<String, String>> redisClusterConnectionMap = new HashMap<>();


    /**
     * 获取standalone命令对象
     * <pre>
     * redis://localhost:16379/0
     * redis://[password@]host[:port][/databaseNumber]
     * redis://[username:password@]host[:port][/databaseNumber]
     * rediss://[[username:]password@]host[:port][/database][?[timeout=timeout[d|h|m|s|ms|us|ns]][&clientName=clientName][&libraryName=libraryName][&libraryVersion=libraryVersion]]
     * redis-socket://[[username:]password@]path[?[timeout=timeout[d|h|m|s|ms|us|ns]][&database=database][&clientName=clientName][&libraryName=libraryName][&libraryVersion=libraryVersion]]
     * </pre>
     *
     * @param uri
     * @return
     */
    public RedisCommands<String, String> standalone(String uri) {
        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> conn = client.connect();
        log.debug("已连接到redis standalone {}", uri);
        redisClientMap.put(uri, client);
        redisConnectionMap.put(uri, conn);
        RedisCommands<String, String> commands = conn.sync();
        return commands;
    }


    /**
     * 获取sentinel命令对象
     * <pre>
     * redis-sentinel://localhost:26379,localhost:26380/0#mymaster
     * redis-sentinel://[password@]host[:port][,host2[:port2]][/databaseNumber]#sentinelMasterId
     * </pre>
     *
     * @param uri
     * @return
     */
    public RedisCommands<String, String> sentinel(String uri) {
        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> conn = client.connect();
        log.debug("已连接到redis sentinel {}", uri);
        redisClientMap.put(uri, client);
        redisConnectionMap.put(uri, conn);
        RedisCommands<String, String> commands = conn.sync();
        return commands;
    }

    /**
     * 获取cluster命令对象
     * <pre>
     * redis://[password@]host[:port]
     * redis://[username:password@]host[:port]
     * </pre>
     *
     * @param uris
     * @return
     */
    public RedisAdvancedClusterCommands<String, String> cluster(List<String> uris) {
        List<RedisURI> uriList = new ArrayList();
        for (String uri : uris) {
            uriList.add(RedisURI.create(uri));
        }
        RedisClusterClient client = RedisClusterClient.create(uriList);
        StatefulRedisClusterConnection<String, String> conn = client.connect();
        String urisString = JSONUtil.toJsonStr(uris);
        log.debug("已连接到redis cluster {}", urisString);
        redisClusterClientMap.put(urisString, client);
        redisClusterConnectionMap.put(urisString, conn);
        RedisAdvancedClusterCommands<String, String> commands = conn.sync();
        return commands;
    }


    /**
     * 获取工具类实例
     *
     * @return
     */
    public static RedisUtil builder() {
        return INSTANCE;
    }

    /**
     * 构建工具类
     *
     * @return
     */
    public RedisUtil build() {
        return INSTANCE;
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        redisConnectionMap.forEach((uri, redisConnection) -> {
            log.debug("关闭链接 {}", uri);
            redisConnection.close();
        });
        redisClientMap.forEach((uri, redisClient) -> {
            log.debug("关闭客户端", uri);
            redisClient.shutdown();
        });
        redisClusterConnectionMap.forEach((uri, redisClusterConnection) -> {
            log.debug("关闭链接 {}", uri);
            redisClusterConnection.close();
        });
        redisClusterClientMap.forEach((uri, redisClusterClient) -> {
            log.debug("关闭客户端", uri);
            redisClusterClient.shutdown();
        });
    }
}
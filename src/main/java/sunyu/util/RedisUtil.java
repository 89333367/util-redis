package sunyu.util;

import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

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
public class RedisUtil implements Serializable, Closeable {
    private Log log = LogFactory.get();
    private static final RedisUtil INSTANCE = new RedisUtil();


    private Map<String, RedisClient> clientMap = new HashMap<>();
    private Map<String, RedisClusterClient> clusterClientMap = new HashMap<>();
    private Map<String, StatefulRedisConnection<String, String>> connectionMap = new HashMap<>();
    private Map<String, StatefulRedisClusterConnection<String, String>> clusterConnectionMap = new HashMap<>();


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
    public StatefulRedisConnection<String, String> standalone(String uri) {
        log.info("构建链接开始 {}", uri);
        if (connectionMap.containsKey(uri)) {
            log.warn("链接已构建，请不要重复构建 {}", uri);
            return connectionMap.get(uri);
        }

        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> conn = client.connect();
        log.info("已连接到redis standalone {}", uri);
        clientMap.put(uri, client);
        connectionMap.put(uri, conn);
        log.info("构建链接成功 {}", uri);
        return connectionMap.get(uri);
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
    public StatefulRedisConnection<String, String> sentinel(String uri) {
        log.info("构建链接开始 {}", uri);
        if (connectionMap.containsKey(uri)) {
            log.warn("链接已构建，请不要重复构建 {}", uri);
            return connectionMap.get(uri);
        }

        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> conn = client.connect();
        log.info("已连接到redis sentinel {}", uri);
        clientMap.put(uri, client);
        connectionMap.put(uri, conn);
        log.info("构建链接成功 {}", uri);
        return connectionMap.get(uri);
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
    public StatefulRedisClusterConnection<String, String> cluster(List<String> uris) {
        String urisStr = JSONUtil.toJsonStr(uris);
        log.info("构建链接开始 {}", urisStr);
        if (clusterConnectionMap.containsKey(urisStr)) {
            log.warn("链接已构建，请不要重复构建 {}", urisStr);
            return clusterConnectionMap.get(urisStr);
        }

        List<RedisURI> uriList = new ArrayList();
        for (String uri : uris) {
            uriList.add(RedisURI.create(uri));
        }
        RedisClusterClient client = RedisClusterClient.create(uriList);
        StatefulRedisClusterConnection<String, String> conn = client.connect();
        log.info("已连接到redis cluster {}", urisStr);
        clusterClientMap.put(urisStr, client);
        clusterConnectionMap.put(urisStr, conn);
        log.info("构建链接成功 {}", urisStr);
        return clusterConnectionMap.get(urisStr);
    }


    /**
     * 私有构造函数，防止外部实例化
     */
    private RedisUtil() {
    }

    /**
     * 获取工具类工厂
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
        log.info("构建工具类开始");
        log.info("构建工具类结束");
        return INSTANCE;
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("销毁redis工具开始");
        connectionMap.forEach((uri, redisConnection) -> {
            try {
                log.info("关闭链接开始 {}", uri);
                redisConnection.close();
                log.info("关闭链接成功 {}", uri);
            } catch (Exception e) {
                log.warn("关闭链接失败 {} {}", uri, e.getMessage());
            }
        });
        clientMap.forEach((uri, redisClient) -> {
            try {
                log.info("关闭客户端开始 {}", uri);
                redisClient.shutdown();
                log.info("关闭客户端成功 {}", uri);
            } catch (Exception e) {
                log.warn("关闭客户端失败 {} {}", uri, e.getMessage());
            }
        });
        clusterConnectionMap.forEach((uris, redisClusterConnection) -> {
            try {
                log.info("关闭链接开始 {}", uris);
                redisClusterConnection.close();
                log.info("关闭链接成功 {}", uris);
            } catch (Exception e) {
                log.warn("关闭链接失败 {} {}", uris, e.getMessage());
            }
        });
        clusterClientMap.forEach((uris, redisClusterClient) -> {
            try {
                log.info("关闭客户端开始 {}", uris);
                redisClusterClient.shutdown();
                log.info("关闭客户端成功 {}", uris);
            } catch (Exception e) {
                log.warn("关闭客户端失败 {} {}", uris, e.getMessage());
            }
        });
        clientMap.clear();
        clusterClientMap.clear();
        connectionMap.clear();
        clusterConnectionMap.clear();
        log.info("销毁redis工具完毕");
    }


}
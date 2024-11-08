package sunyu.util;

import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

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
    private final Log log = LogFactory.get();
    private static final RedisUtil INSTANCE = new RedisUtil();

    private final Map<String, RedisClient> clientMap = new HashMap<>();
    private final Map<String, RedisClusterClient> clusterClientMap = new HashMap<>();
    private final Map<String, StatefulRedisConnection<String, String>> standaloneConnectionMap = new HashMap<>();
    private final Map<String, StatefulRedisMasterReplicaConnection<String, String>> sentinelConnectionMap = new HashMap<>();
    private final Map<String, StatefulRedisClusterConnection<String, String>> clusterConnectionMap = new HashMap<>();

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
        if (standaloneConnectionMap.containsKey(uri)) {
            log.warn("链接已构建，请不要重复构建 {}", uri);
            return standaloneConnectionMap.get(uri);
        }

        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> conn = client.connect();
        log.info("已连接到redis standalone {}", uri);
        clientMap.put(uri, client);
        standaloneConnectionMap.put(uri, conn);
        log.info("构建链接成功 {}", uri);
        return standaloneConnectionMap.get(uri);
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
    public StatefulRedisMasterReplicaConnection<String, String> sentinel(String uri) {
        log.info("构建链接开始 {}", uri);
        if (sentinelConnectionMap.containsKey(uri)) {
            log.warn("链接已构建，请不要重复构建 {}", uri);
            return sentinelConnectionMap.get(uri);
        }


        RedisClient client = RedisClient.create();

        log.info("构建主从链接开始");
        StatefulRedisMasterReplicaConnection<String, String> conn = MasterReplica.connect(client, StringCodec.UTF8, RedisURI.create(uri));
        log.info("构建主从链接完毕");
        log.info("设置读取策略开始");
        conn.setReadFrom(ReadFrom.UPSTREAM_PREFERRED);
        log.info("设置读取策略完毕 {}", conn.getReadFrom());

        log.info("已连接到redis sentinel {}", uri);
        clientMap.put(uri, client);
        sentinelConnectionMap.put(uri, conn);
        log.info("构建链接成功 {}", uri);
        return sentinelConnectionMap.get(uri);
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

        List<RedisURI> uriList = new ArrayList<>();
        for (String uri : uris) {
            uriList.add(RedisURI.create(uri));
        }
        RedisClusterClient client = RedisClusterClient.create(uriList);

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
        client.setOptions(clusterClientOptions);
        log.info("设置集群客户端参数结束");

        StatefulRedisClusterConnection<String, String> conn = client.connect();

        log.info("设置读取策略开始");
        conn.setReadFrom(ReadFrom.REPLICA_PREFERRED);//设置为从副本中读取首选值，如果没有可用的副本，则回退到上游。
        log.info("设置读取策略完毕 {}", conn.getReadFrom());

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
        standaloneConnectionMap.forEach((uri, conn) -> {
            try {
                log.info("关闭链接开始 {}", uri);
                conn.close();
                log.info("关闭链接成功 {}", uri);
            } catch (Exception e) {
                log.warn("关闭链接失败 {} {}", uri, ExceptionUtil.stacktraceToString(e));
            }
        });
        sentinelConnectionMap.forEach((uris, conn) -> {
            try {
                log.info("关闭链接开始 {}", uris);
                conn.close();
                log.info("关闭链接成功 {}", uris);
            } catch (Exception e) {
                log.warn("关闭链接失败 {} {}", uris, ExceptionUtil.stacktraceToString(e));
            }
        });
        clusterConnectionMap.forEach((uris, conn) -> {
            try {
                log.info("关闭链接开始 {}", uris);
                conn.close();
                log.info("关闭链接成功 {}", uris);
            } catch (Exception e) {
                log.warn("关闭链接失败 {} {}", uris, ExceptionUtil.stacktraceToString(e));
            }
        });
        clientMap.forEach((uri, client) -> {
            try {
                log.info("关闭客户端开始 {}", uri);
                client.shutdown();
                log.info("关闭客户端成功 {}", uri);
            } catch (Exception e) {
                log.warn("关闭客户端失败 {} {}", uri, ExceptionUtil.stacktraceToString(e));
            }
        });
        clusterClientMap.forEach((uris, client) -> {
            try {
                log.info("关闭客户端开始 {}", uris);
                client.shutdown();
                log.info("关闭客户端成功 {}", uris);
            } catch (Exception e) {
                log.warn("关闭客户端失败 {} {}", uris, ExceptionUtil.stacktraceToString(e));
            }
        });

        standaloneConnectionMap.clear();
        sentinelConnectionMap.clear();
        clusterConnectionMap.clear();
        clientMap.clear();
        clusterClientMap.clear();

        log.info("销毁redis工具完毕");
    }


}
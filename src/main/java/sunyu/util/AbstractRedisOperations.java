package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.*;

import java.util.List;
import java.util.function.Consumer;

/**
 * Redis 操作抽象类
 * <p>
 * 该类提供了对 Redis 各种数据类型操作的抽象封装，基于 Lettuce 客户端实现
 * 支持字符串、哈希、列表、集合、有序集合、地理空间、HyperLogLog、流等多种数据类型的操作
 * </p>
 *
 * <p>
 * 泛型参数说明：
 * <ul>
 * <li>K - Redis 键的类型</li>
 * <li>V - Redis 值的类型</li>
 * <li>T - 继承自多个 Redis 命令接口的具体命令实现类，用于执行实际的 Redis 命令</li>
 * </ul>
 * </p>
 *
 * <p>
 * 主要功能：
 * <ul>
 * <li>字符串操作：获取值（get）</li>
 * <li>键操作：扫描键（scan）</li>
 * <li>地理空间操作：添加地理位置（geoadd）、根据经纬度查找最近的成员（georadiusWithCountOne）</li>
 * </ul>
 * </p>
 *
 * <p>
 * 使用方式：
 * 1. 继承此类并实现 getCommands() 方法，返回具体的 Redis 命令实现
 * 2. 调用提供的方法执行相应的 Redis 操作
 * </p>
 *
 * @param <K> Redis 键的类型
 * @param <V> Redis 值的类型
 * @param <T> Redis 命令实现类，需继承自多个 Redis 命令接口
 * @author SunYu
 */
public abstract class AbstractRedisOperations<K, V, T extends
        RedisGeoCommands<K, V>
        & RedisHashCommands<K, V>
        & RedisHLLCommands<K, V>
        & RedisKeyCommands<K, V>
        & RedisListCommands<K, V>
        & RedisScriptingCommands<K, V>
        & RedisServerCommands<K, V>
        & RedisSetCommands<K, V>
        & RedisSortedSetCommands<K, V>
        & RedisStreamCommands<K, V>
        & RedisStringCommands<K, V>> {
    public abstract T getCommands();

    /**
     * 获取值
     *
     * @param key 键
     * @return 值
     */
    public V get(K key) {
        return getCommands().get(key);
    }

    /**
     * 扫描key
     *
     * @param match   可以使用*匹配
     * @param limit   限制每批读取多少条，建议500
     * @param handler 处理器
     */
    public void scan(K match, int limit, Consumer<K> handler) {
        T commands = getCommands();
        KeyScanCursor<K> scanCursor = null;
        ScanArgs scanArgs = new ScanArgs().match(match.toString()).limit(limit);
        do {
            scanCursor = (scanCursor == null) ? commands.scan(scanArgs) : commands.scan(scanCursor, scanArgs);
            for (K key : scanCursor.getKeys()) {
                handler.accept(key);
            }
        } while (!scanCursor.isFinished());
    }

    /**
     * 获取指定经纬度距离最近的一个成员
     *
     * @param key 键
     * @param lon 经度
     * @param lat 纬度
     * @param m   距离(米)
     * @return 成员
     */
    public V georadiusWithCountOne(K key, double lon, double lat, double m) {
        // GEORADIUS places {longitude} {latitude} {radius} m COUNT 1 ASC
        GeoArgs args = new GeoArgs()
                .asc() // 按距离升序排列
                .withCount(1); // 只返回最近的1个结果
        List<GeoWithin<V>> results;
        T commands = getCommands();
        results = commands.georadius(
                key, // key
                lon, // 经度
                lat, // 纬度
                m, // 距离
                GeoArgs.Unit.m, // 单位（米）
                args // 参数配置
        );
        if (CollUtil.isNotEmpty(results)) {
            return results.get(0).getMember();
        }
        return null;
    }

    /**
     * 添加经纬度
     *
     * @param key    键
     * @param lon    经度
     * @param lat    纬度
     * @param member 地址信息
     */
    public void geoadd(K key, double lon, double lat, V member) {
        // GEOADD places {longitude} {latitude} "{address_name}"
        T commands = getCommands();
        commands.geoadd(key, lon, lat, member);
    }

}
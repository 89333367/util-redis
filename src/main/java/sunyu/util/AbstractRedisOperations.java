package sunyu.util;

import java.util.List;

import cn.hutool.core.collection.CollUtil;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;

public abstract class AbstractRedisOperations<T extends RedisStringCommands<String, String> & RedisKeyCommands<String, String> & RedisGeoCommands<String, String>> {
    public abstract T getCommands();

    /**
     * 获取值
     *
     * @param key 键
     *
     * @return 值
     */
    public String get(String key) {
        return getCommands().get(key);
    }

    /**
     * 扫描
     *
     * @param match   可以使用*匹配
     * @param limit   限制每批读取多少条，建议500
     * @param handler 处理器
     */
    public void scan(String match, int limit, java.util.function.Consumer<String> handler) {
        T commands = getCommands();
        KeyScanCursor<String> scanCursor = null;
        ScanArgs scanArgs = new ScanArgs().match(match).limit(limit);
        do {
            scanCursor = (scanCursor == null) ? commands.scan(scanArgs) : commands.scan(scanCursor, scanArgs);
            for (String key : scanCursor.getKeys()) {
                handler.accept(key);
            }
        } while (!scanCursor.isFinished());
    }

    /**
     * 获取指定经纬度距离最近的成员
     *
     * @param key 键
     * @param lon 经度
     * @param lat 纬度
     * @param m   距离(米)
     *
     * @return 成员
     */
    public String getMemberBygeoradius(String key, double lon, double lat, double m) {
        // GEORADIUS places {longitude} {latitude} {radius} m COUNT 1 ASC
        GeoArgs args = new GeoArgs()
                .asc() // 按距离升序排列
                .withCount(1); // 只返回最近的1个结果
        List<GeoWithin<String>> results;
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
     * @param member 成员
     */
    public void geoadd(String key, double lon, double lat, String member) {
        // GEOADD places {longitude} {latitude} "{address_name}"
        T commands = getCommands();
        commands.geoadd(key, lon, lat, member);
    }

}
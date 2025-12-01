# Redis工具类

## 描述

完全使用lettuce来操作redis，封装的目的是改变了包名称，避免冲突

## 环境

* jdk8 x64 及以上版本

## 依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-redis</artifactId>
    <!-- {lettuce-core.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <version>7.1.0.RELEASE_1.0_jdk8_x64</version>
    <classifier>shaded</classifier>
</dependency>
```

## 例子

```java
@Test
void testGet() {
    RedisUtil redisUtil = RedisUtil.builder().uri("redis://192.168.11.39:16379/0").build();
    String v = redisUtil.getCommands().get("subsidy:bc:userinfo");
    log.info(v);
    v = redisUtil.get("subsidy:bc:userinfo");
    log.info(v);
    redisUtil.close();

    /*RedisSentinelUtil redisSentinelUtil = RedisSentinelUtil.builder().uri("redis-sentinel://localhost:26379,localhost:26380/0#mymaster").build();
    v = redisSentinelUtil.getCommands().get("subsidy:bc:userinfo");
    log.info(v);
    v = redisSentinelUtil.get("subsidy:bc:userinfo");
    log.info(v);
    redisSentinelUtil.close();*/
}

@Test
void testMget() {
    RedisClusterUtil redisClusterUtil = RedisClusterUtil.builder()
            .nodes("192.168.11.124:7001,192.168.11.124:7002,192.168.11.124:7003,192.168.11.125:7004,192.168.11.125:7005,192.168.11.125:7006")
            .build();

    for (KeyValue<String, String> kv : redisClusterUtil.getCommands().mget("relation:16200442", "farm:realtime:0865306056453850", "abc")) {
        if (kv.isEmpty()) {
            log.debug("{}", kv.getKey());
        } else {
            log.debug("{} {}", kv.getKey(), kv.getValue());
        }
    }

    redisClusterUtil.close();
}

@Test
void testScan() {
    RedisClusterUtil redisClusterUtil = RedisClusterUtil.builder()
            .uriStrList(Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                    "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"))
            .build();

    redisClusterUtil.scan("ne:realtime:*", 500, key -> {
        log.info("{}", key);
    });

    redisClusterUtil.close();
}


@Test
void testGeoradius() {
    RedisUtil redisUtil = RedisUtil.builder()
            .uri("redis://11KYms98qm@192.168.13.73:30794/2")
            .build();

    // GEOADD places {longitude} {latitude} "{address_name}"
    redisUtil.geoadd("pca", 116.37304, 39.92594, "北京市西城区什刹海街道西什库大街19号院");

    // GEORADIUS places {longitude} {latitude} {radius} m COUNT 1 ASC
    String member = redisUtil.georadiusWithCountOne("pca", 116.37304, 39.92594, 10);
    log.info(member);

    redisUtil.close();
}
```


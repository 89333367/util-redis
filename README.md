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
    <!-- {kafka-clients.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <version>6.5.5.RELEASE_1.0_jdk8_x64</version>
</dependency>
```

## 例子

```java
@Test
void testGet() {
    RedisStandaloneUtil redisStandaloneUtil = RedisStandaloneUtil.builder().uri("redis://192.168.11.39:16379/0").build();
    String v = redisStandaloneUtil.getCommands().get("subsidy:bc:userinfo");
    log.info(v);
    v = redisStandaloneUtil.get("subsidy:bc:userinfo");
    log.info(v);
    redisStandaloneUtil.close();

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
```


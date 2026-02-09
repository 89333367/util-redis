# Redis工具类

## 描述

封装了lettuce的redis操作，更改了lettuce里面netty的包名，避免与项目中的netty组件冲突

## 环境

* jdk8 x64 及以上版本

## 依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-redis</artifactId>
    <!-- {lettuce-core.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <!-- 6.0.9.RELEASE是最后一个支持jdk8的版本 -->
    <version>6.0.9.RELEASE_1.1_jdk8_x64</version>
    <classifier>shaded</classifier>
</dependency>
```

## 例子

```
redis :// [[username :] password@] host [:port][/database]
          [?[timeout=timeout[d|h|m|s|ms|us|ns]] [&clientName=clientName]
          [&libraryName=libraryName] [&libraryVersion=libraryVersion] ]
          
redis-sentinel :// [[username :] password@] host1[:port1] [, host2[:port2]] [, hostN[:portN]] [/database]
                   [?[timeout=timeout[d|h|m|s|ms|us|ns]] [&sentinelMasterId=sentinelMasterId]
                   [&clientName=clientName] [&libraryName=libraryName]
                   [&libraryVersion=libraryVersion] ]

redis.standalone.uri=redis://rPo4IdPmg9@172.16.1.180:31091/2

redis.cluster.uri=redis://172.16.1.29:7001,redis://172.16.1.10:7002,redis://172.16.1.26:7003,redis://172.16.1.25:7004,redis://172.16.1.11:7005,redis://172.16.1.19:7006
redis.cluster.uri=172.16.1.29:7001,172.16.1.10:7002,172.16.1.26:7003,172.16.1.25:7004,172.16.1.11:7005,172.16.1.19:7006
```

```java

@Test
void testCluster() {
    RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
            .nodes(props.getStr("redis.cluster.nodes"))
            .build();

    String v = clusterUtil.get("p:r:d:600243");
    log.info("{}", v);

    clusterUtil.close();
}

@Test
void testScan() {
    RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
            .nodes(props.getStr("redis.cluster.nodes"))
            .build();

    clusterUtil.scan("p:r:v:*", 500, key -> log.info("{}", key));

    clusterUtil.close();
}

@Test
void testMget() {
    RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
            .nodes(props.getStr("redis.cluster.nodes"))
            .build();

    Map<String, String> kvs = clusterUtil.mget("farm:realtime:600044", "abc", "farm:realtime:600179");
    kvs.forEach((k, v) -> log.info("{} {}", k, v));

    clusterUtil.close();
}

@Test
void testMget2() {
    RedisClusterUtil clusterUtil = new RedisClusterUtil.Builder()
            .nodes(props.getStr("redis.cluster.nodes"))
            .build();

    List<KeyValue<String, String>> lvs = clusterUtil.getCommands().mget("farm:realtime:600044", "abc", "farm:realtime:600179");
    // 检查lvs是否为null
    if (lvs != null) {
        for (KeyValue<String, String> kv : lvs) {
            // 旧版Lettuce没有isEmpty方法，需要手动检查KeyValue是否有效
            // 通过检查hasValue()和getValue()是否为null来判断
            if (kv != null && kv.hasValue() && kv.getValue() != null) {
                // 安全日志输出，避免空指针
                log.info("{} {}", kv.getKey() != null ? kv.getKey() : "null", kv.getValue());
            } else {
                // KeyValue无效或值为null的情况
                if (kv != null) {
                    log.info("{} null", kv.getKey() != null ? kv.getKey() : "null");
                } else {
                    log.info("KeyValue is null");
                }
            }
        }
    } else {
        log.info("MGET returned null");
    }

    clusterUtil.close();
}

@Test
void testStandalone() {
    RedisUtil standaloneUtil = new RedisUtil.Builder()
            .uri(props.getStr("redis.standalone.uri"))
            .build();
    String v = standaloneUtil.georadiusWithCountOne("pca:tianditu", 86.018138, 28.283572, 1000);
    log.info("{}", v);

    standaloneUtil.close();
}
```


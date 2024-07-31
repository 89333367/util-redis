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
    <version>lettuce-6.4.0.RELEASE_v1.0</version>
</dependency>
```

## 例子

```java

@Test
void t004() {
    RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
    RedisCommands<String, String> standalone = redisUtil.standalone("redis://192.168.11.39:16379/0");//全局只需要一个

    log.debug(standalone.get("subsidy:bc:userinfo"));

    redisUtil.close();//如果程序不再使用了，可以调用这个
}

@Test
void t005() {
    RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
    RedisAdvancedClusterCommands<String, String> cluster = redisUtil.cluster(
            Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                    "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

    for (KeyValue<String, String> kv : cluster.mget("relation:16200442", "farm:realtime:0865306056453850")) {
        log.debug("{} {}", kv.getKey(), kv.getValue());
    }

    redisUtil.close();//如果程序不再使用了，可以调用这个
}
```


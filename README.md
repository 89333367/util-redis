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
    <version>lettuce-6.5.0.RELEASE_v1.0</version>
</dependency>
```

## 例子

```java

@Test
void t004() {
    RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
    StatefulRedisConnection<String, String> standalone = redisUtil.standalone("redis://192.168.11.39:16379/0");//全局只需要一个

    log.debug(standalone.sync().get("subsidy:bc:userinfo"));

    redisUtil.close();//如果程序不再使用了，可以调用这个
}

@Test
void t005() {
    RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
    StatefulRedisClusterConnection<String, String> cluster = redisUtil.cluster(
            Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                    "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

    for (KeyValue<String, String> kv : cluster.sync().mget("relation:16200442", "farm:realtime:0865306056453850")) {
        if (kv.isEmpty()) {
            log.debug("{}", kv);
        } else {
            log.debug("{} {}", kv.getKey(), kv.getValue());
        }
    }

    redisUtil.close();//如果程序不再使用了，可以调用这个
}


@Test
void t007() {
    RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
    StatefulRedisConnection<String, String> standalone = redisUtil.standalone("redis://192.168.11.39:16379/0");//全局只需要一个

    //异步调用
    RedisFuture<String> stringRedisFuture = standalone.async().get("subsidy:bc:userinfo");
    try {
        String v = stringRedisFuture.get();//等待异步方法返回
        log.info("{}", v);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    } catch (ExecutionException e) {
        throw new RuntimeException(e);
    }

    redisUtil.close();//如果程序不再使用了，可以调用这个
}

@Test
void t008() {
    RedisUtil redisUtil = RedisUtil.builder().build();//全局只需要一个
    StatefulRedisClusterConnection<String, String> cluster = redisUtil.cluster(
            Arrays.asList("redis://192.168.11.124:7001", "redis://192.168.11.124:7002", "redis://192.168.11.124:7003",
                    "redis://192.168.11.125:7004", "redis://192.168.11.125:7005", "redis://192.168.11.125:7006"));//全局只需要一个

    //异步调用
    RedisFuture<List<KeyValue<String, String>>> mget = cluster.async().mget("relation:16200442", "farm:realtime:0865306056453850");
    try {
        for (KeyValue<String, String> kv : mget.get()) {
            if (kv.isEmpty()) {
                log.debug("{}", kv);
            } else {
                log.debug("{} {}", kv.getKey(), kv.getValue());
            }
        }
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    } catch (ExecutionException e) {
        throw new RuntimeException(e);
    }

    redisUtil.close();//如果程序不再使用了，可以调用这个
}
```


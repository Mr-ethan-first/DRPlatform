package com.example.remotedatasync.cdc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CDC 延迟/进度注册表（JVM 内单例）：各作业的 sink subtask 更新，
 * 供监控 API / 前端展示「实时同步延迟」。
 *
 * <p>延迟定义：源端事件提交时间（Debezium ts_ms）与最近一次落库的墙钟差。
 * 判据：lag &lt;= {@code lagWarnSeconds}（默认 10s）视为满足实时性要求。
 *
 * @author 50707
 */
public final class CdcLagRegistry {

    private CdcLagRegistry() {
    }

    private static final class Entry {

        volatile long lastEventTsMs;

        volatile long lastApplyWallMs;

        volatile long appliedTotal;

        volatile long lastUpdateWallMs = System.currentTimeMillis();
    }

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    private static volatile int lagWarnSeconds = 10;

    public static void configure(int warnSeconds) {
        lagWarnSeconds = warnSeconds;
    }

    /** sink 收到事件时调用（记录源端事件时间） */
    public static void update(String instanceKey, long eventTsMs) {
        Entry e = ENTRIES.computeIfAbsent(instanceKey, k -> new Entry());
        if (eventTsMs > e.lastEventTsMs) {
            e.lastEventTsMs = eventTsMs;
        }
    }

    /** sink 落库一批后调用 */
    public static void applied(String instanceKey, long total) {
        Entry e = ENTRIES.get(instanceKey);
        if (e != null) {
            e.appliedTotal = total;
            e.lastApplyWallMs = System.currentTimeMillis();
            e.lastUpdateWallMs = e.lastApplyWallMs;
        }
    }

    public static long lagSeconds(String instanceKey) {
        Entry e = ENTRIES.get(instanceKey);
        if (e == null || e.lastEventTsMs <= 0) {
            return -1;
        }
        return (e.lastApplyWallMs - e.lastEventTsMs) / 1000;
    }

    public static boolean isLagging(String instanceKey) {
        long lag = lagSeconds(instanceKey);
        return lag > lagWarnSeconds;
    }

    public static long appliedTotal(String instanceKey) {
        Entry e = ENTRIES.get(instanceKey);
        return e == null ? 0 : e.appliedTotal;
    }

    public static void remove(String instanceKey) {
        ENTRIES.remove(instanceKey);
    }

    public static Map<String, Object> snapshot(String instanceKey) {
        Entry e = ENTRIES.get(instanceKey);
        Map<String, Object> m = new ConcurrentHashMap<>();
        if (e == null) {
            m.put("lagSeconds", -1);
            m.put("appliedTotal", 0L);
            return m;
        }
        m.put("lagSeconds", lagSeconds(instanceKey));
        m.put("appliedTotal", e.appliedTotal);
        m.put("lastEventTsMs", e.lastEventTsMs);
        m.put("lastApplyWallMs", e.lastApplyWallMs);
        m.put("lagWarnSeconds", lagWarnSeconds);
        m.put("lagging", isLagging(instanceKey));
        return m;
    }
}

package com.aliyun.ha3engine.jdbc.common.config;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3KvPairBuilder {

    private StringBuilder kvPairs;

    public Ha3KvPairBuilder setTrace(String value) {
        return addKvPair("trace", value);
    }

    public Ha3KvPairBuilder setFormatType(String value) {
        return addKvPair("formatType", value);
    }

    // 单位 ms
    public Ha3KvPairBuilder setTimeout(int value) {
        return addKvPair("timeout", value);
    }

    // 把kvpair的布尔值搞成两个开关函数，For True
    public Ha3KvPairBuilder enableSearchInfo() {
        return addKvPair("searchInfo", true);
    }

    // For false
    public Ha3KvPairBuilder disableSearchInfo() {
        return addKvPair("searchInfo", false);
    }

    public Ha3KvPairBuilder enableSqlPlan() {
        return addKvPair("sqlPlan", true);
    }

    public Ha3KvPairBuilder disableSqlPlan() {
        return addKvPair("sqlPlan", false);
    }

    public Ha3KvPairBuilder enableForbitMergeSearchInfo() {
        return addKvPair("forbitMergeSearchInfo", true);
    }

    public Ha3KvPairBuilder disableForbitMergeSearchInfo() {
        return addKvPair("forbitMergeSearchInfo", false);
    }

    public Ha3KvPairBuilder enableResultReadable() {
        return addKvPair("resultReadable", true);
    }

    public Ha3KvPairBuilder disableResultReadable() {
        return addKvPair("resultReadable", false);
    }

    public Ha3KvPairBuilder setParallel(int value) {
        return addKvPair("parallel", value);
    }

    public Ha3KvPairBuilder setParallelTables(String value) {
        return addKvPair("parallelTables", value);
    }

    public Ha3KvPairBuilder setDatabaseName(String value) {
        return addKvPair("databaseName", value);
    }

    public Ha3KvPairBuilder setCatalogName(String value) {
        return addKvPair("catalogName", value);
    }

    public Ha3KvPairBuilder enableLackResultEnable() {
        return addKvPair("lackResultEnable", true);
    }

    public Ha3KvPairBuilder disableLackResultEnable() {
        return addKvPair("lackResultEnable", false);
    }

    public Ha3KvPairBuilder enableDebug() {
        return addKvPair("iquan.optimizer.debug.enable", true);
    }

    public Ha3KvPairBuilder disableDebug() {
        return addKvPair("iquan.optimizer.debug.enable", false);
    }

    public Ha3KvPairBuilder enableForceSortLimitEnable() {
        return addKvPair("iquan.optimizer.sort.limit.use.together", true);
    }

    public Ha3KvPairBuilder disableForceSortLimitEnable() {
        return addKvPair("iquan.optimizer.sort.limit.use.together", false);
    }

    public Ha3KvPairBuilder enableForceLimit() {
        return addKvPair("iquan.optimizer.force.limit.enable", true);
    }

    public Ha3KvPairBuilder disableForceLimit() {
        return addKvPair("iquan.optimizer.force.limit.enable", false);
    }

    public Ha3KvPairBuilder setForceLimitNum(long forceLimitNum) {
        return addKvPair("iquan.optimizer.force.limit.num", forceLimitNum);
    }

    public Ha3KvPairBuilder enableJoinConditionCheck() {
        return addKvPair("iquan.optimizer.join.condition.check", true);
    }

    public Ha3KvPairBuilder disableJoinConditionCheck() {
        return addKvPair("iquan.optimizer.join.condition.check", false);
    }

    public Ha3KvPairBuilder enableForceHashJoin() {
        return addKvPair("iquan.optimizer.join.condition.check", true);
    }

    public Ha3KvPairBuilder disableForceHashJoin() {
        return addKvPair("iquan.optimizer.join.condition.check", false);
    }

    public Ha3KvPairBuilder setFormatVersion(String value) {
        return addKvPair("iquan.plan.format.version", value);
    }

    public Ha3KvPairBuilder setPlanFormatType(String value) {
        return addKvPair("iquan.plan.format.type", value);
    }

    public Ha3KvPairBuilder setPrepareLevel(String value) {
        return addKvPair("iquan.plan.prepare.level", value);
    }

    public Ha3KvPairBuilder enableCache() {
        return addKvPair("iquan.plan.cache.enable", true);
    }

    public Ha3KvPairBuilder disableCache() {
        return addKvPair("iquan.plan.cache.enable", false);
    }

    public Ha3KvPairBuilder setSourceId(String value) {
        return addKvPair("exec.source.id", value);
    }

    public Ha3KvPairBuilder setSourceSpec(String value) {
        return addKvPair("exec.source.spec", value);
    }

    public Ha3KvPairBuilder setDynamicParams() {
        return addKvPair("dynamic_params", "dynamic_params");
    }

    public Ha3KvPairBuilder enableUrlEncodeData() {
        return addKvPair("urlencode_data", true);
    }

    public Ha3KvPairBuilder disableUrlEncodeData() {
        return addKvPair("urlencode_data", false);
    }

    public Ha3KvPairBuilder setIquanOptimizerLevel(String value) {
        return addKvPair("iquan.optimizer.level", value);
    }

    public Ha3KvPairBuilder addKvPair(String key, Object value) {
        if (kvPairs == null) {
            kvPairs = new StringBuilder();
        }

        if (this.kvPairs.length() != 0) {
            this.kvPairs.append(";");
        }
        this.kvPairs.append(key).append(":").append(value);
        return this;
    }

    public String getKvPairString() {
        return kvPairs.toString();
    }

}

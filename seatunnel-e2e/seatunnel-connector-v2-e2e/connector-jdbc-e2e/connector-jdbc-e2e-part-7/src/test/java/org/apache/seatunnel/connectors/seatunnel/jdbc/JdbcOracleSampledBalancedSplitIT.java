/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.SampledBalancedChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * SampledBalancedChunkSplitter Oracle E2E 测试
 *
 * 测试目标：
 * 1. 验证采样分片算法在真实 Oracle 数据库环境下的表现
 * 2. 测试数据倾斜场景下的分片均衡性
 * 3. 验证不同数据分布的处理能力
 * 4. 确保端到端数据处理的正确性
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcOracleSampledBalancedSplitIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOracleSampledBalancedSplitIT.class);

    // 容器相关常量保持不变
    private static final String ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "oracle-sampled-e2e";
    private static final int ORACLE_PORT = 1521;

    // 测试表定义
    private static final String UNIFORM_TABLE = "UNIFORM_DATA";
    private static final String SKEWED_TABLE = "SKEWED_DATA";

    // 静态变量，在容器启动后初始化
    private static String ACTUAL_USERNAME;
    private static String ACTUAL_PASSWORD;
    private static String ACTUAL_SCHEMA;
    private static String JDBC_URL;

    // 实例变量
    private OracleContainer oracle_container;
    private Connection connection;
    private OracleCatalog catalog;

    private static final int UNIFORM_DATA_SIZE = 10000;
    private static final int SKEWED_DATA_SIZE = 10000;

    // 定义全局变量
    private static final double SAMPLING_PERCENTAGE = 1.0;
    private static final int BUCKET_NUMBER = 5;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        initContainer();
        initializeConnectionInfo(); // 初始化连接信息
        given().await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(this::initializeTestTables);
    }

    void initContainer() {
        DockerImageName imageName = DockerImageName.parse(ORACLE_IMAGE);

        oracle_container = new OracleContainer(imageName)
                .withNetwork(NETWORK)
                .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                .withExposedPorts(ORACLE_PORT)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));

        oracle_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, 1521)));

        Startables.deepStart(Stream.of(oracle_container)).join();
        LOG.info("Oracle 容器启动完成");
    }

    /**
     * 初始化连接信息 - 在容器启动后调用
     */
    private void initializeConnectionInfo() {
        JDBC_URL = oracle_container.getJdbcUrl();
        ACTUAL_USERNAME = oracle_container.getUsername();
        ACTUAL_PASSWORD = oracle_container.getPassword();
        ACTUAL_SCHEMA = ACTUAL_USERNAME.toUpperCase();

        LOG.info("连接信息初始化完成: URL={}, User={}, Schema={}",
                JDBC_URL, ACTUAL_USERNAME, ACTUAL_SCHEMA);
    }

    private void initializeTestTables() {
        try {
            connection = DriverManager.getConnection(JDBC_URL, ACTUAL_USERNAME, ACTUAL_PASSWORD);

            catalog = new OracleCatalog("oracle", ACTUAL_USERNAME, ACTUAL_PASSWORD,
                    OracleURLParser.parse(JDBC_URL), ACTUAL_SCHEMA);
            catalog.open();

            createUniformDataTable();
            createSkewedDataTable();

        } catch (Exception e) {
            LOG.error("初始化测试表失败", e);
            throw new RuntimeException("初始化测试表失败", e);
        }
    }

    @Test
    @Order(1)
    public void testSampledBalancedSplit_uniformData() throws Exception {
        LOG.info("=== 测试均匀分布数据的采样分片效果 (Oracle) ===");

        Map<String, Object> configMap = createSampledBalancedConfig();
        configMap.put("table_path", ACTUAL_SCHEMA + "." + UNIFORM_TABLE);
        configMap.put("sampling_percentage", SAMPLING_PERCENTAGE);
        configMap.put("bucket_number", BUCKET_NUMBER);

        SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
        Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, UNIFORM_TABLE);

        // 验证逻辑保持不变
        Assertions.assertEquals(BUCKET_NUMBER, splits.size());
        validateDataCompleteness(splits, UNIFORM_DATA_SIZE);
        double balanceScore = calculateBalanceScore(splits);
        Assertions.assertTrue(balanceScore > 0.7, "均匀数据的均衡性得分应该 > 0.7");
    }

    @Test
    @Order(2)
    public void testSampledBalancedSplit_skewedData() throws Exception {
        LOG.info("=== 测试倾斜分布数据的采样分片效果 (Oracle) ===");

        Map<String, Object> configMap = createSampledBalancedConfig();
        configMap.put("table_path", ACTUAL_SCHEMA + "." + SKEWED_TABLE);
        configMap.put("sampling_percentage", SAMPLING_PERCENTAGE);
        configMap.put("bucket_number", BUCKET_NUMBER);

        SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
        Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, SKEWED_TABLE);

        Assertions.assertEquals(BUCKET_NUMBER, splits.size());
        validateDataCompleteness(splits, SKEWED_DATA_SIZE);
        double balanceScore = calculateBalanceScore(splits);
        Assertions.assertTrue(balanceScore > 0.5, "采样分片应该能改善倾斜数据的均衡性");
    }

    @Test
    @Order(3)
    public void testDifferentSamplingRates() throws Exception {
        LOG.info("=== 测试不同采样比例的效果 (Oracle) ===");
        // 对于单元测试来说，由于数据量比较小，所以可能会有balanceScore比较低的情况
        double[] samplingRates = {1.0, 0.5, 0.1};

        for (double rate : samplingRates) {
            Map<String, Object> configMap = createSampledBalancedConfig();
            configMap.put("table_path", ACTUAL_SCHEMA + "." + UNIFORM_TABLE);
            configMap.put("sampling_percentage", rate);
            configMap.put("bucket_number", BUCKET_NUMBER);

            SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
            Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, UNIFORM_TABLE);

            Assertions.assertEquals(BUCKET_NUMBER, splits.size());
            validateDataCompleteness(splits, UNIFORM_DATA_SIZE);
            double balanceScore = calculateBalanceScore(splits);
            LOG.info("rate: {} , balanceScore: {}", rate, balanceScore);
        }
    }

    private Map<String, Object> createSampledBalancedConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", JDBC_URL);
        configMap.put("driver", "oracle.jdbc.OracleDriver");
        configMap.put("user", ACTUAL_USERNAME);
        configMap.put("password", ACTUAL_PASSWORD);
        configMap.put("split.size", "200");
        return configMap;
    }

    private Collection<JdbcSourceSplit> executeSplitGeneration(SampledBalancedChunkSplitter splitter,
                                                               String tableName) throws Exception {
        TablePath tablePath = TablePath.of(null, ACTUAL_SCHEMA, tableName);

        Assertions.assertTrue(catalog.tableExists(tablePath));
        CatalogTable catalogTable = catalog.getTable(tablePath);

        JdbcSourceTable jdbcSourceTable = JdbcSourceTable.builder()
                .tablePath(tablePath)
                .catalogTable(catalogTable)
                .build();

        return splitter.generateSplits(jdbcSourceTable);
    }
    /**
     * 创建均匀分布测试数据 - Oracle 版本
     */
    private void createUniformDataTable() {
        String createSql = String.format(
                "CREATE TABLE %s (" +
                        "id NUMBER(10) PRIMARY KEY, " +
                        "name VARCHAR2(50), " +
                        "value NUMBER(10,2), " +
                        "category VARCHAR2(20)" +
                        ")", UNIFORM_TABLE);

        String insertSql = String.format(
                "INSERT INTO %s (id, name, value, category) VALUES (?, ?, ?, ?)", UNIFORM_TABLE);

        try (PreparedStatement createStmt = connection.prepareStatement(createSql);
             PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {

            createStmt.execute();

            // 使用简单的循环生成均匀分布数据
            for (int i = 0; i < UNIFORM_DATA_SIZE; i++) {
                insertStmt.setInt(1, i);
                insertStmt.setString(2, "uniform_name_" + i);
                insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 1.5));
                insertStmt.setString(4, "category_" + (i % 10));
                insertStmt.addBatch();

                // Oracle 批处理建议每1000条执行一次
                if (i % 1000 == 0 || i == UNIFORM_DATA_SIZE - 1) {
                    insertStmt.executeBatch();
                }
            }

            // Oracle 统计信息收集
            try (Statement analyzeStmt = connection.createStatement()) {
                analyzeStmt.execute("ANALYZE TABLE " + UNIFORM_TABLE + " COMPUTE STATISTICS");
            }

            LOG.info("已创建均匀分布数据表 {}, 记录数: {}", UNIFORM_TABLE, UNIFORM_DATA_SIZE);

        } catch (SQLException e) {
            LOG.error("创建均匀数据表失败", e);
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, e);
        }
    }

    /**
     * 创建倾斜分布测试数据 - Oracle 版本
     */
    private void createSkewedDataTable() {
        String createSql = String.format(
                "CREATE TABLE %s (" +
                        "id NUMBER(19) PRIMARY KEY, " +
                        "business_type VARCHAR2(20), " +
                        "amount NUMBER(15,2), " +
                        "region VARCHAR2(20)" +
                        ")", SKEWED_TABLE);

        String insertSql = String.format(
                "INSERT INTO %s (id, business_type, amount, region) VALUES (?, ?, ?, ?)", SKEWED_TABLE);

        try (PreparedStatement createStmt = connection.prepareStatement(createSql);
             PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {

            createStmt.execute();

            int denseCount = (int) (SKEWED_DATA_SIZE * 0.8);
            int sparseCount = SKEWED_DATA_SIZE - denseCount;

            // 确保ID唯一性
            long currentId = 1;

            // 密集区域：连续的ID 1-800
            for (int i = 0; i < denseCount; i++) {
                insertStmt.setLong(1, currentId++);
                insertStmt.setString(2, "dense_business");
                insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 10.5));
                insertStmt.setString(4, "hot_region");
                insertStmt.addBatch();

                if (i % 1000 == 0 || i == denseCount - 1) {
                    insertStmt.executeBatch();
                }
            }

            // 稀疏区域：跳跃式ID分布
            currentId = 10000; // 从10000开始，制造稀疏效果
            for (int i = 0; i < sparseCount; i++) {
                insertStmt.setLong(1, currentId);
                insertStmt.setString(2, "sparse_business");
                insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 25.8));
                insertStmt.setString(4, "cold_region");
                insertStmt.addBatch();

                currentId += 100; // 每次跳跃100，制造稀疏分布

                if (i % 1000 == 0 || i == sparseCount - 1) {
                    insertStmt.executeBatch();
                }
            }

            // Oracle 统计信息收集
            try (Statement analyzeStmt = connection.createStatement()) {
                analyzeStmt.execute("ANALYZE TABLE " + SKEWED_TABLE + " COMPUTE STATISTICS");
            }

            LOG.info("已创建倾斜分布数据表 {}, 总记录数: {}, 密集区域ID: 1-{}, 稀疏区域ID: {}",
                    SKEWED_TABLE, SKEWED_DATA_SIZE, denseCount, sparseCount);

        } catch (SQLException e) {
            LOG.error("创建倾斜数据表失败", e);
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, e);
        }
    }

    // ==================== 辅助方法 ====================

//    private Map<String, Object> createSampledBalancedConfig() {
//        Map<String, Object> configMap = new HashMap<>();
//
//        String jdbcUrl = String.format(ORACLE_URL_TEMPLATE,
//                oracle_container.getHost(),
//                oracle_container.getMappedPort(ORACLE_PORT),
//                DATABASE);
//
//        configMap.put("url", jdbcUrl);
//        configMap.put("driver", "oracle.jdbc.OracleDriver");
//        configMap.put("user", USERNAME);
//        configMap.put("password", PASSWORD);
//        configMap.put("split.size", "200");  // 每个分片目标大小
//
//        return configMap;
//    }


    @NotNull
    private SampledBalancedChunkSplitter getSampledBalancedSplitter(Map<String, Object> configMap) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(readonlyConfig);
        return new SampledBalancedChunkSplitter(sourceConfig);
    }


    /**
     * 验证数据完整性：所有分片应该覆盖完整数据集，无重复无遗漏
     */
    private void validateDataCompleteness(Collection<JdbcSourceSplit> splits, int expectedTotal) {
        Set<Long> processedIds = new HashSet<>();

        for (JdbcSourceSplit split : splits) {
            Set<Long> splitIds = executeQueryAndGetIds(split);

            // 检查重复数据
            for (Long id : splitIds) {
                Assertions.assertFalse(processedIds.contains(id),
                        "发现重复数据 ID: " + id + " 在分片: " + split.getSplitId());
                processedIds.add(id);
            }
        }

        // 验证总数
        Assertions.assertEquals(expectedTotal, processedIds.size(),
                "数据总量不匹配，期望: " + expectedTotal + ", 实际: " + processedIds.size());
    }

    /**
     * 执行分片查询并获取所有 ID
     */
    private Set<Long> executeQueryAndGetIds(JdbcSourceSplit split) {
        Set<Long> ids = new HashSet<>();
        String sql = buildSplitQuery(split);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setSplitParameters(stmt, split);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong("id"));
                }
            }

        } catch (SQLException e) {
            LOG.error("执行分片查询失败: " + sql, e);
            throw new RuntimeException(e);
        }

        return ids;
    }

    /**
     * 构建分片查询 SQL - Oracle 版本
     */
    private String buildSplitQuery(JdbcSourceSplit split) {
        String tableName = quoteIdentifier(split.getTablePath().getSchemaName()) + "." +
                quoteIdentifier(split.getTablePath().getTableName());
        String baseQuery = "SELECT id FROM " + tableName;

        if (split.getSplitKeyName() == null) {
            return baseQuery;
        }

        String splitKey = quoteIdentifier(split.getSplitKeyName());
        boolean hasStart = split.getSplitStart() != null;
        boolean hasEnd = split.getSplitEnd() != null;

        if (!hasStart && !hasEnd) {
            return baseQuery;
        } else if (!hasStart) {
            return baseQuery + " WHERE " + splitKey + " <= ?";
        } else if (!hasEnd) {
            return baseQuery + " WHERE " + splitKey + " > ?";
        } else {
            return baseQuery + " WHERE " + splitKey + " > ? AND " + splitKey + " <= ?";
        }
    }

    /**
     * Oracle 字段引用
     */
    private String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    /**
     * 设置分片查询参数
     */
    private void setSplitParameters(PreparedStatement stmt, JdbcSourceSplit split) throws SQLException {
        boolean hasStart = split.getSplitStart() != null;
        boolean hasEnd = split.getSplitEnd() != null;

        if (!hasStart && !hasEnd) {
            return;
        }

        int paramIndex = 1;
        if (hasStart) {
            stmt.setObject(paramIndex++, split.getSplitStart());
        }
        if (hasEnd) {
            stmt.setObject(paramIndex, split.getSplitEnd());
        }
    }

    /**
     * 计算分片均衡性得分
     */
    private double calculateBalanceScore(Collection<JdbcSourceSplit> splits) {
        List<Integer> splitSizes = new ArrayList<>();

        for (JdbcSourceSplit split : splits) {
            int size = executeQueryAndGetIds(split).size();
            splitSizes.add(size);
            LOG.debug("分片 {} 数据量: {}", split.getSplitId(), size);
        }

        if (splitSizes.isEmpty()) {
            return 0.0;
        }

        // 计算均值
        double average = splitSizes.stream().mapToInt(Integer::intValue).average().orElse(0);

        // 计算标准差
        double variance = splitSizes.stream()
                .mapToDouble(size -> Math.pow(size - average, 2))
                .average()
                .orElse(0);
        double stdDev = Math.sqrt(variance);

        // 计算变异系数并转换为均衡性得分
        if (average == 0) {
            return splitSizes.stream().allMatch(size -> size == 0) ? 1.0 : 0.0;
        }

        double coefficientOfVariation = stdDev / average;
        return Math.max(0, 1 - coefficientOfVariation);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            if (catalog != null) {
                catalog.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("关闭连接时出现异常", e);
        }

        if (oracle_container != null) {
            oracle_container.close();
        }
        LOG.info("Oracle 容器已清理完成");
    }
}
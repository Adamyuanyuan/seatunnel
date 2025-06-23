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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.SampledBalancedChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.AfterAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SampledBalancedChunkSplitter E2E 测试 - 本地MySQL版本
 *
 * 测试目标：
 * 1. 验证采样分片算法在真实数据库环境下的表现
 * 2. 测试数据倾斜场景下的分片均衡性
 * 3. 验证不同数据分布的处理能力
 * 4. 确保端到端数据处理的正确性
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcMysqlSampledBalancedSplitITForLocal {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMysqlSampledBalancedSplitIT.class);

    private static final String MYSQL_DATABASE = "sampled_test";

    // 测试表定义
    private static final String UNIFORM_TABLE = "uniform_data";      // 均匀分布数据表
    private static final String SKEWED_TABLE = "skewed_data";        // 倾斜分布数据表

    private static final String MYSQL_USERNAME = "test_user";
    private static final String MYSQL_PASSWORD = "test_user_pw";
    private static final int MYSQL_PORT = 12306;

    // 本地MySQL连接配置 - 可通过系统属性覆盖
    private static final String MYSQL_HOST = "localhost";

    private static final int UNIFORM_DATA_SIZE = 1000;   // 减少数据量提升测试速度
    private static final int SKEWED_DATA_SIZE = 1000;    // 减少数据量提升测试速度

    private static String jdbcUrl;

    @BeforeAll
    static void setUp() throws Exception {
        // 构建JDBC URL
        jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE);

        LOG.info("连接到本地MySQL: {}", jdbcUrl);

        // 确保数据库存在
        ensureDatabaseExists();

        // 初始化测试表
        initializeTestTables();

        LOG.info("测试环境初始化完成");
    }

    @AfterAll
    static void tearDown() throws Exception {
        // 清理测试数据
        cleanupTestTables();
        LOG.info("测试环境清理完成");
    }

    /**
     * 确保数据库存在
     */
    private static void ensureDatabaseExists() throws SQLException {
        String baseUrl = String.format("jdbc:mysql://%s:%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                MYSQL_HOST, MYSQL_PORT);

        try (Connection conn = DriverManager.getConnection(baseUrl, MYSQL_USERNAME, MYSQL_PASSWORD)) {
            String createDbSql = "CREATE DATABASE IF NOT EXISTS " + MYSQL_DATABASE + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            conn.prepareStatement(createDbSql).execute();
            LOG.info("数据库 {} 已准备就绪", MYSQL_DATABASE);
        }
    }

    /**
     * 初始化测试表
     */
    private static void initializeTestTables() throws SQLException {
        createUniformDataTable();
        createSkewedDataTable();
    }

    /**
     * 清理测试表
     */
    private static void cleanupTestTables() throws SQLException {
        try (Connection conn = getJdbcConnection()) {
            conn.prepareStatement("DROP TABLE IF EXISTS " + UNIFORM_TABLE).execute();
            conn.prepareStatement("DROP TABLE IF EXISTS " + SKEWED_TABLE).execute();
            LOG.info("测试表已清理");
        }
    }

    /**
     * 创建均匀分布测试数据
     */
    private static void createUniformDataTable() throws SQLException {
        String createSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id INT PRIMARY KEY, " +
                        "name VARCHAR(50), " +
                        "value DECIMAL(10,2), " +
                        "category VARCHAR(20)" +
                        ")", UNIFORM_TABLE);

        String insertSql = String.format(
                "INSERT INTO %s (id, name, value, category) VALUES (?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE name=VALUES(name)", UNIFORM_TABLE);

        try (Connection conn = getJdbcConnection()) {
            // 创建表
            conn.prepareStatement(createSql).execute();

            // 检查是否已有数据
            String countSql = "SELECT COUNT(*) FROM " + UNIFORM_TABLE;
            try (PreparedStatement countStmt = conn.prepareStatement(countSql);
                 ResultSet rs = countStmt.executeQuery()) {
                rs.next();
                if (rs.getInt(1) >= UNIFORM_DATA_SIZE) {
                    LOG.info("均匀数据表 {} 已存在足够数据，跳过插入", UNIFORM_TABLE);
                    return;
                }
            }

            // 清空并重新插入
            conn.prepareStatement("DELETE FROM " + UNIFORM_TABLE).execute();

            try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                for (int i = 1; i <= UNIFORM_DATA_SIZE; i++) {
                    insertStmt.setInt(1, i);
                    insertStmt.setString(2, "uniform_name_" + i);
                    insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 1.5));
                    insertStmt.setString(4, "category_" + (i % 10));
                    insertStmt.addBatch();

                    if (i % 100 == 0) {
                        insertStmt.executeBatch();
                    }
                }
                insertStmt.executeBatch();
            }

            LOG.info("已创建均匀分布数据表 {}, 记录数: {}", UNIFORM_TABLE, UNIFORM_DATA_SIZE);
        }
    }

    /**
     * 创建倾斜分布测试数据
     */
    private static void createSkewedDataTable() throws SQLException {
        String createSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id BIGINT PRIMARY KEY, " +
                        "business_type VARCHAR(20), " +
                        "amount DECIMAL(15,2), " +
                        "region VARCHAR(20)" +
                        ")", SKEWED_TABLE);

        String insertSql = String.format(
                "INSERT INTO %s (id, business_type, amount, region) VALUES (?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE business_type=VALUES(business_type)", SKEWED_TABLE);

        try (Connection conn = getJdbcConnection()) {
            // 创建表
            conn.prepareStatement(createSql).execute();

            // 检查是否已有数据
            String countSql = "SELECT COUNT(*) FROM " + SKEWED_TABLE;
            try (PreparedStatement countStmt = conn.prepareStatement(countSql);
                 ResultSet rs = countStmt.executeQuery()) {
                rs.next();
                if (rs.getInt(1) >= SKEWED_DATA_SIZE) {
                    LOG.info("倾斜数据表 {} 已存在足够数据，跳过插入", SKEWED_TABLE);
                    return;
                }
            }

            // 清空并重新插入
            conn.prepareStatement("DELETE FROM " + SKEWED_TABLE).execute();

            try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                int denseCount = (int) (SKEWED_DATA_SIZE * 0.8);
                int sparseCount = SKEWED_DATA_SIZE - denseCount;

                long currentId = 1;

                // 密集区域：连续的ID
                for (int i = 0; i < denseCount; i++) {
                    insertStmt.setLong(1, currentId++);
                    insertStmt.setString(2, "dense_business");
                    insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 10.5));
                    insertStmt.setString(4, "hot_region");
                    insertStmt.addBatch();

                    if (i % 100 == 0) {
                        insertStmt.executeBatch();
                    }
                }

                // 稀疏区域：跳跃式ID分布
                currentId = 10000;
                for (int i = 0; i < sparseCount; i++) {
                    insertStmt.setLong(1, currentId);
                    insertStmt.setString(2, "sparse_business");
                    insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 25.8));
                    insertStmt.setString(4, "cold_region");
                    insertStmt.addBatch();

                    currentId += 100;

                    if (i % 50 == 0) {
                        insertStmt.executeBatch();
                    }
                }

                insertStmt.executeBatch();
                LOG.info("已创建倾斜分布数据表 {}, 总记录数: {}, 密集区域: {}, 稀疏区域: {}",
                        SKEWED_TABLE, SKEWED_DATA_SIZE, denseCount, sparseCount);
            }


        }
    }

    @Test
    @Order(1)
    public void testSampledBalancedSplit_uniformData() throws Exception {
        LOG.info("=== 测试均匀分布数据的采样分片效果 ===");

        Map<String, Object> configMap = createSampledBalancedConfig();
        configMap.put("table_path", MYSQL_DATABASE + "." + UNIFORM_TABLE);
        configMap.put("sampling_percentage", 0.1);  // 10% 采样率
        configMap.put("bucket_number", 4);  // 减少分片数量

        SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
        Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, UNIFORM_TABLE);

        // 验证分片数量
        Assertions.assertEquals(4, splits.size());
        LOG.info("均匀数据分片数量验证通过: {}", splits.size());

        // 验证数据完整性
        validateDataCompleteness(splits, UNIFORM_DATA_SIZE);
        LOG.info("均匀数据完整性验证通过");

        // 验证数据均衡性
        double balanceScore = calculateBalanceScore(splits);
        LOG.info("均匀数据均衡性得分: {}", balanceScore);
        Assertions.assertTrue(balanceScore > 0.7, "均匀数据的均衡性得分应该 > 0.7");
    }

    @Test
    @Order(2)
    public void testSampledBalancedSplit_skewedData() throws Exception {
        LOG.info("=== 测试倾斜分布数据的采样分片效果 ===");

        Map<String, Object> configMap = createSampledBalancedConfig();
        configMap.put("table_path", MYSQL_DATABASE + "." + SKEWED_TABLE);
        configMap.put("sampling_percentage", 0.05);  // 5% 采样率
        configMap.put("partition_num", 4);  // 减少分片数量

        SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
        Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, SKEWED_TABLE);

        // 验证分片数量
        Assertions.assertEquals(4, splits.size());
        LOG.info("倾斜数据分片数量验证通过: {}", splits.size());

        // 验证数据完整性
        validateDataCompleteness(splits, SKEWED_DATA_SIZE);
        LOG.info("倾斜数据完整性验证通过");

        // 验证均衡性改进效果
        double balanceScore = calculateBalanceScore(splits);
        LOG.info("倾斜数据均衡性得分: {}", balanceScore);

        // 对于倾斜数据，采样分片应该能显著改善均衡性
        Assertions.assertTrue(balanceScore > 0.4, "采样分片应该能改善倾斜数据的均衡性");
    }

    @Test
    @Order(3)
    public void testDifferentSamplingRates() throws Exception {
        LOG.info("=== 测试不同采样比例的效果 ===");

        double[] samplingRates = {0.05, 0.1};  // 减少测试用例

        for (double rate : samplingRates) {
            LOG.info("测试采样率: {}", rate);

            Map<String, Object> configMap = createSampledBalancedConfig();
            configMap.put("table_path", MYSQL_DATABASE + "." + UNIFORM_TABLE);
            configMap.put("sampling_percentage", rate);
            configMap.put("partition_num", 3);  // 减少分片数量

            SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);

            long startTime = System.currentTimeMillis();
            Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, UNIFORM_TABLE);
            long endTime = System.currentTimeMillis();

            // 验证基本结果
            Assertions.assertEquals(3, splits.size());
            validateDataCompleteness(splits, UNIFORM_DATA_SIZE);

            double balanceScore = calculateBalanceScore(splits);
            LOG.info("采样率 {} - 均衡性得分: {}, 耗时: {}ms", rate, balanceScore, endTime - startTime);

            // 所有采样率都应该保持基本的均衡性
            Assertions.assertTrue(balanceScore > 0.5,
                    "采样率 " + rate + " 的均衡性应该保持在合理水平");
        }
    }

    // ==================== 辅助方法 ====================

    private Map<String, Object> createSampledBalancedConfig() {
        Map<String, Object> configMap = new HashMap<>();
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(jdbcUrl);

        configMap.put("url", urlInfo.getUrlWithDatabase().get());
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");
        configMap.put("user", MYSQL_USERNAME);
        configMap.put("password", MYSQL_PASSWORD);
        configMap.put("split.size", "150");  // 调整分片大小

        return configMap;
    }

    @NotNull
    private SampledBalancedChunkSplitter getSampledBalancedSplitter(Map<String, Object> configMap) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(readonlyConfig);
        return new SampledBalancedChunkSplitter(sourceConfig);
    }

    private Collection<JdbcSourceSplit> executeSplitGeneration(SampledBalancedChunkSplitter splitter,
                                                               String tableName) throws Exception {
        TablePath tablePath = TablePath.of(MYSQL_DATABASE, tableName);

        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(jdbcUrl);
        MySqlCatalog catalog = new MySqlCatalog("mysql", MYSQL_USERNAME, MYSQL_PASSWORD, urlInfo);
        catalog.open();

        try {
            Assertions.assertTrue(catalog.tableExists(tablePath));
            CatalogTable catalogTable = catalog.getTable(tablePath);

            JdbcSourceTable jdbcSourceTable = JdbcSourceTable.builder()
                    .tablePath(tablePath)
                    .catalogTable(catalogTable)
                    .build();

            return splitter.generateSplits(jdbcSourceTable);

        } finally {
            catalog.close();
        }
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

        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

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
     * 构建分片查询 SQL
     */
    private String buildSplitQuery(JdbcSourceSplit split) {
        String tableName = split.getTablePath().getTableName();
        String baseQuery = "SELECT id FROM " + tableName;

        if (split.getSplitKeyName() == null) {
            return baseQuery;
        }

        String splitKey = "`" + split.getSplitKeyName() + "`";
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

    private static Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, MYSQL_USERNAME, MYSQL_PASSWORD);
    }
}
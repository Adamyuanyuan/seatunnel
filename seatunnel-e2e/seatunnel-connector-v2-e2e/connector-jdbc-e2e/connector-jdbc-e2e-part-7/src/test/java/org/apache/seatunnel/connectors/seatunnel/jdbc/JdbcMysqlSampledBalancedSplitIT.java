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
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.SampledBalancedChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.apache.commons.lang3.tuple.Pair;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * SampledBalancedChunkSplitter E2E 测试
 *
 * 测试目标：
 * 1. 验证采样分片算法在真实数据库环境下的表现
 * 2. 测试数据倾斜场景下的分片均衡性
 * 3. 验证不同数据分布的处理能力
 * 4. 确保端到端数据处理的正确性
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcMysqlSampledBalancedSplitIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMysqlSampledBalancedSplitIT.class);

    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_CONTAINER_HOST = "mysql-sampled-e2e";
    private static final String MYSQL_DATABASE = "sampled_test";

    // 测试表定义
    private static final String UNIFORM_TABLE = "uniform_data";      // 均匀分布数据表
    private static final String SKEWED_TABLE = "skewed_data";        // 倾斜分布数据表

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3313;

    private MySQLContainer<?> mysql_container;

    private static final int UNIFORM_DATA_SIZE = 1000;  // 均匀数据总量
    private static final int SKEWED_DATA_SIZE = 1000;   // 倾斜数据总量

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        initContainer();
        given().await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeTestTables);
    }

    void initContainer() throws ClassNotFoundException {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);
        mysql_container =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        mysql_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, 3306)));

        Startables.deepStart(Stream.of(mysql_container)).join();
    }

    private void initializeTestTables() {
        // 创建均匀分布数据表
        createUniformDataTable();
        // 创建倾斜分布数据表
        createSkewedDataTable();
    }

    /**
     * 创建均匀分布测试数据 - 简化版本
     */
    private void createUniformDataTable() {
        String createSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id INT PRIMARY KEY, " +
                        "name VARCHAR(50), " +
                        "value DECIMAL(10,2), " +
                        "category VARCHAR(20)" +
                        ")", UNIFORM_TABLE);

        String insertSql = String.format(
                "INSERT INTO %s (id, name, value, category) VALUES (?, ?, ?, ?)", UNIFORM_TABLE);

        try (Connection conn = getJdbcConnection();
             PreparedStatement createStmt = conn.prepareStatement(createSql);
             PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {

            createStmt.execute();

            // 🔥 借鉴成功方案：使用简单的循环生成
            for (int i = 0; i < UNIFORM_DATA_SIZE; i++) {
                insertStmt.setInt(1, i);
                insertStmt.setString(2, "uniform_name_" + i);
                insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 1.5));
                insertStmt.setString(4, "category_" + (i % 10));
                insertStmt.addBatch();
            }

            // 一次性批量插入
            insertStmt.executeBatch();

            // 更新统计信息
            conn.prepareStatement("ANALYZE TABLE " + UNIFORM_TABLE).execute();
            LOG.info("已创建均匀分布数据表 {}, 记录数: {}", UNIFORM_TABLE, UNIFORM_DATA_SIZE);

        } catch (SQLException e) {
            LOG.error("创建均匀数据表失败", e);
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, e);
        }
    }


    /**
     * 创建倾斜分布测试数据 - 修复版本
     */
    private void createSkewedDataTable() {
        String createSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id BIGINT PRIMARY KEY, " +
                        "business_type VARCHAR(20), " +
                        "amount DECIMAL(15,2), " +
                        "region VARCHAR(20)" +
                        ")", SKEWED_TABLE);

        String insertSql = String.format(
                "INSERT INTO %s (id, business_type, amount, region) VALUES (?, ?, ?, ?)", SKEWED_TABLE);

        try (Connection conn = getJdbcConnection();
             PreparedStatement createStmt = conn.prepareStatement(createSql);
             PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {

            createStmt.execute();

            int denseCount = (int) (SKEWED_DATA_SIZE * 0.8);
            int sparseCount = SKEWED_DATA_SIZE - denseCount;

            // 🔥 修复方案：确保ID唯一性
            long currentId = 1;

            // 密集区域：连续的ID 1-640
            for (int i = 0; i < denseCount; i++) {
                insertStmt.setLong(1, currentId++);
                insertStmt.setString(2, "dense_business");
                insertStmt.setBigDecimal(3, BigDecimal.valueOf(i * 10.5));
                insertStmt.setString(4, "hot_region");
                insertStmt.addBatch();
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
            }

            insertStmt.executeBatch();
            conn.prepareStatement("ANALYZE TABLE " + SKEWED_TABLE).execute();

            LOG.info("已创建倾斜分布数据表 {}, 总记录数: {}, 密集区域ID: 1-{}, 稀疏区域ID: {}",
                    SKEWED_TABLE, SKEWED_DATA_SIZE, denseCount, sparseCount);

        } catch (SQLException e) {
            LOG.error("创建倾斜数据表失败", e);
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, e);
        }
    }


    @Test
    @Order(1)
    public void testSampledBalancedSplit_uniformData() throws Exception {
        LOG.info("=== 测试均匀分布数据的采样分片效果 ===");

        Map<String, Object> configMap = createSampledBalancedConfig();
        configMap.put("table_path", MYSQL_DATABASE + "." + UNIFORM_TABLE);
        configMap.put("sampling_percentage", 0.1);  // 10% 采样率
        configMap.put("bucket_number", 5);

        SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
        Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, UNIFORM_TABLE);

        // 验证分片数量
        Assertions.assertEquals(5, splits.size());
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
        configMap.put("bucket_number", 5);

        SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);
        Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, SKEWED_TABLE);

        // 验证分片数量
        Assertions.assertEquals(5, splits.size());
        LOG.info("倾斜数据分片数量验证通过: {}", splits.size());

        // 验证数据完整性
        validateDataCompleteness(splits, SKEWED_DATA_SIZE);
        LOG.info("倾斜数据完整性验证通过");

        // 验证均衡性改进效果
        double balanceScore = calculateBalanceScore(splits);
        LOG.info("倾斜数据均衡性得分: {}", balanceScore);

        // 对于倾斜数据，采样分片应该能显著改善均衡性
        Assertions.assertTrue(balanceScore > 0.5, "采样分片应该能改善倾斜数据的均衡性");
    }

    @Test
    @Order(3)
    public void testDifferentSamplingRates() throws Exception {
        LOG.info("=== 测试不同采样比例的效果 ===");

        double[] samplingRates = {0.01, 0.1, 1};  // 1%, 5%, 10%

        for (double rate : samplingRates) {
            LOG.info("测试采样率: {}", rate);

            Map<String, Object> configMap = createSampledBalancedConfig();
            configMap.put("table_path", MYSQL_DATABASE + "." + UNIFORM_TABLE);
            configMap.put("sampling_percentage", rate);
            configMap.put("bucket_number", 5);

            SampledBalancedChunkSplitter splitter = getSampledBalancedSplitter(configMap);

            long startTime = System.currentTimeMillis();
            Collection<JdbcSourceSplit> splits = executeSplitGeneration(splitter, UNIFORM_TABLE);
            long endTime = System.currentTimeMillis();

            // 验证基本结果
            Assertions.assertEquals(5, splits.size());
            validateDataCompleteness(splits, UNIFORM_DATA_SIZE);

            double balanceScore = calculateBalanceScore(splits);
            LOG.info("采样率 {} - 均衡性得分: {}, 耗时: {}ms", rate, balanceScore, endTime - startTime);

            // 所有采样率都应该保持基本的均衡性
            Assertions.assertTrue(balanceScore > 0.6,
                    "采样率 " + rate + " 的均衡性应该保持在合理水平");
        }
    }

    // ==================== 辅助方法 ====================

    private Map<String, Object> createSampledBalancedConfig() {
        Map<String, Object> configMap = new HashMap<>();
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(
                String.format("jdbc:mysql://localhost:%s/%s?useSSL=false", MYSQL_PORT, MYSQL_DATABASE));

        configMap.put("url", urlInfo.getUrlWithDatabase().get());
        configMap.put("driver", "com.mysql.cj.jdbc.Driver");
        configMap.put("user", MYSQL_USERNAME);
        configMap.put("password", MYSQL_PASSWORD);
        configMap.put("split.size", "200");  // 每个分片目标大小

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

        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(
                String.format("jdbc:mysql://localhost:%s/%s?useSSL=false", MYSQL_PORT, MYSQL_DATABASE));
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

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                mysql_container.getJdbcUrl(),
                mysql_container.getUsername(),
                mysql_container.getPassword());
    }

    @Override
    public void tearDown() throws Exception {
        LOG.info("容器保持运行状态，以便下次测试复用");
        if (mysql_container != null) {
            mysql_container.close();
            dockerClient.removeContainerCmd(mysql_container.getContainerId()).exec();
        }
    }
}
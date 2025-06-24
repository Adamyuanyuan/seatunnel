/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * SampledBalancedChunkSplitter 单元测试类
 *
 * 测试采样均衡分片器的核心功能：
 * 1. SQL 查询语句生成逻辑
 * 2. 分片创建和边界计算逻辑
 * 3. PreparedStatement 参数设置逻辑
 * 4. 各种边界条件的处理
 */
public class SampledBalancedChunkSplitterTest {

    private SampledBalancedChunkSplitter splitter;

    @Mock private JdbcSourceConfig mockJdbcSourceConfig;
    @Mock private JdbcConnectionConfig mockJdbcConnectionConfig;
    @Mock private JdbcDialect mockJdbcDialect;
    @Mock private JdbcConnectionProvider mockJdbcConnectionProvider;
    @Mock private Connection mockConnection;
    @Mock private PreparedStatement mockPreparedStatement;

    private JdbcSourceTable basicJdbcSourceTable;
    private TableSchema basicTableSchema;

    // 测试常量定义
    private static final String SPLIT_KEY_NAME = "id";
    private static final SeaTunnelDataType<?> SPLIT_KEY_TYPE = BasicType.INT_TYPE;
    private static final TablePath BASIC_TABLE_PATH = TablePath.of("mydb", "myschema", "mytable");
    private static final int DEFAULT_PARTITION_NUM = 5;
    private static final int DEFAULT_FETCH_SIZE = 1000;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // 1. 配置基础 Mock 对象
        configureMockObjects();

        // 2. 创建分片器实例
        createSplitterInstance();

        // 3. 通过反射注入 Mock 方言对象
        injectMockDialect();

        // 4. 设置 Mock 行为
        setupMockBehaviors();

        // 5. 创建测试数据
        createTestData();
    }

    /**
     * 配置基础的 Mock 对象行为
     */
    private void configureMockObjects() {
        when(mockJdbcSourceConfig.getJdbcConnectionConfig()).thenReturn(mockJdbcConnectionConfig);
        when(mockJdbcConnectionConfig.getUrl()).thenReturn("jdbc:mysql://localhost:3306/test");
        when(mockJdbcConnectionConfig.getCompatibleMode()).thenReturn(null);
        when(mockJdbcSourceConfig.getFetchSize()).thenReturn(DEFAULT_FETCH_SIZE);
        when(mockJdbcSourceConfig.getSamplingPercentage()).thenReturn(0.001);
        when(mockJdbcSourceConfig.getBucketNumber()).thenReturn(5);  // 关键配置
        when(mockJdbcSourceConfig.isSampledBalancedSharding()).thenReturn(true);
    }

    /**
     * 创建可控制的分片器实例，重写关键方法以便测试
     */
    private void createSplitterInstance() {
        splitter = new SampledBalancedChunkSplitter(mockJdbcSourceConfig) {
            @Override
            protected Connection getOrEstablishConnection() throws SQLException {
                return mockConnection;
            }

            @Override
            protected PreparedStatement createPreparedStatement(String sql) throws SQLException {
                return mockPreparedStatement;
            }
        };
    }

    /**
     * 通过反射将 Mock 方言对象注入到分片器中
     * 这是解决基类构造函数中创建真实方言实例问题的关键步骤
     */
    private void injectMockDialect() throws Exception {
        Field dialectField = ChunkSplitter.class.getDeclaredField("jdbcDialect");
        dialectField.setAccessible(true);
        dialectField.set(splitter, mockJdbcDialect);
    }

    /**
     * 设置 Mock 对象的行为模拟
     */
    private void setupMockBehaviors() throws SQLException, ClassNotFoundException {
        // Mock 方言方法
        when(mockJdbcDialect.quoteIdentifier(SPLIT_KEY_NAME))
                .thenReturn("`" + SPLIT_KEY_NAME + "`");
        when(mockJdbcDialect.tableIdentifier(BASIC_TABLE_PATH))
                .thenReturn("`mydb`.`myschema`.`mytable`");
        when(mockJdbcDialect.creatPreparedStatement(any(), anyString(), anyInt()))
                .thenReturn(mockPreparedStatement);

        // Mock 连接提供者
        when(mockJdbcDialect.getJdbcConnectionProvider(mockJdbcConnectionConfig))
                .thenReturn(mockJdbcConnectionProvider);
        when(mockJdbcConnectionProvider.getOrEstablishConnection())
                .thenReturn(mockConnection);
    }

    /**
     * 创建测试所需的表结构和数据对象
     */
    private void createTestData() {
        // 创建表结构
        List<Column> columns = Arrays.asList(
                PhysicalColumn.of(SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 20L, true, null, "id column"),
                PhysicalColumn.of("name", BasicType.STRING_TYPE, 255L, true, null, "name column")
        );

        basicTableSchema = TableSchema.builder()
                .primaryKey(null)
                .constraintKey(Collections.emptyList())
                .columns(columns)
                .build();

        // 创建 TableIdentifier 和 CatalogTable
        TableIdentifier tableIdentifier = TableIdentifier.of(
                "test_catalog",
                BASIC_TABLE_PATH.getDatabaseName(),
                BASIC_TABLE_PATH.getSchemaName(),
                BASIC_TABLE_PATH.getTableName()
        );

        CatalogTable catalogTable = CatalogTable.of(
                tableIdentifier,
                basicTableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "test table"
        );

        // 创建 JdbcSourceTable
        basicJdbcSourceTable = JdbcSourceTable.builder()
                .tablePath(BASIC_TABLE_PATH)
                .query(null)
                .partitionColumn(SPLIT_KEY_NAME)
                .partitionNumber(DEFAULT_PARTITION_NUM)
                .catalogTable(catalogTable)
                .build();
    }

    /**
     * 获取分片键类型的辅助方法
     */
    private SeaTunnelRowType getSplitKeyType() {
        return new SeaTunnelRowType(
                new String[]{SPLIT_KEY_NAME},
                new SeaTunnelDataType<?>[]{SPLIT_KEY_TYPE}
        );
    }

    // ==================== SQL 生成测试 ====================

    /**
     * 测试目的：验证首个分片（起始边界为 null）的 SQL 查询语句生成是否正确
     * 期望：生成 WHERE column <= ? 格式的 SQL
     */
    @Test
    void testCreateSampledBalancedSplitQuerySQL_firstSplit() {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_0", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, null, 100);

        String sql = splitter.createSampledBalancedSplitQuerySQL(split, basicTableSchema);

        assertEquals(
                "SELECT * FROM `mydb`.`myschema`.`mytable` WHERE `" + SPLIT_KEY_NAME + "` <= ?",
                sql);
    }

    /**
     * 测试目的：验证中间分片（既有起始边界又有结束边界）的 SQL 查询语句生成是否正确
     * 期望：生成 WHERE column > ? AND column <= ? 格式的 SQL
     */
    @Test
    void testCreateSampledBalancedSplitQuerySQL_middleSplit() {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_1", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 100, 200);

        String sql = splitter.createSampledBalancedSplitQuerySQL(split, basicTableSchema);

        assertEquals(
                "SELECT * FROM `mydb`.`myschema`.`mytable` WHERE `" + SPLIT_KEY_NAME +
                        "` > ? AND `" + SPLIT_KEY_NAME + "` <= ?",
                sql);
    }

    /**
     * 测试目的：验证最后分片（结束边界为 null）的 SQL 查询语句生成是否正确
     * 期望：生成 WHERE column > ? 格式的 SQL
     */
    @Test
    void testCreateSampledBalancedSplitQuerySQL_lastSplit() {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_2", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 200, null);

        String sql = splitter.createSampledBalancedSplitQuerySQL(split, basicTableSchema);

        assertEquals(
                "SELECT * FROM `mydb`.`myschema`.`mytable` WHERE `" + SPLIT_KEY_NAME + "` > ?",
                sql);
    }

    /**
     * 测试目的：验证单个分片（起始和结束边界都为 null）的 SQL 查询语句生成是否正确
     * 期望：生成不含 WHERE 条件的基础 SELECT 语句
     */
    @Test
    void testCreateSampledBalancedSplitQuerySQL_singleSplit() {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_0", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, null, null);

        String sql = splitter.createSampledBalancedSplitQuerySQL(split, basicTableSchema);

        assertEquals("SELECT * FROM `mydb`.`myschema`.`mytable`", sql);
    }

    /**
     * 测试目的：验证当存在自定义查询语句时，SQL 查询语句生成是否正确
     * 期望：将自定义查询作为子查询，并在外层添加 WHERE 条件
     */
    @Test
    void testCreateSampledBalancedSplitQuerySQL_withOriginalQuery() {
        String originalQuery = "SELECT id, name FROM mytable_custom_query WHERE active = 1";

        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_1", originalQuery, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 100, 200);

        String sql = splitter.createSampledBalancedSplitQuerySQL(split, basicTableSchema);

        assertEquals(
                "SELECT * FROM (" + originalQuery + ") tmp WHERE `" + SPLIT_KEY_NAME +
                        "` > ? AND `" + SPLIT_KEY_NAME + "` <= ?",
                sql);
    }

    // ==================== 分片创建测试 ====================

    /**
     * 测试目的：验证当采样未返回任何边界值时，是否正确回退到单个分片
     * 期望：创建一个没有分片键和边界的单个分片
     */
    @Test
    void testCreateSplits_noBoundaries() throws Exception {
        when(mockJdbcDialect.sampleAndCalculateBoundaries(
                any(Connection.class), any(JdbcSourceTable.class),
                anyString(), anyDouble(), anyInt()))
                .thenReturn(new Object[0]);

        Collection<JdbcSourceSplit> splits = splitter.createSplits(basicJdbcSourceTable, getSplitKeyType());

        assertNotNull(splits);
        assertEquals(1, splits.size());

        JdbcSourceSplit singleSplit = splits.iterator().next();
        assertNull(singleSplit.getSplitStart());
        assertNull(singleSplit.getSplitEnd());
        // 验证分片ID是否正确生成
        assertTrue(singleSplit.getSplitId().contains(BASIC_TABLE_PATH.toString()));
    }

    /**
     * 测试目的：验证当采样返回一个边界值时，是否正确创建两个分片
     * 期望：创建 [null, boundary] 和 [boundary, null] 两个分片
     */
    @Test
    void testCreateSplits_oneBoundary() throws Exception {
        Object[] boundaries = new Object[]{150};
        when(mockJdbcDialect.sampleAndCalculateBoundaries(
                any(Connection.class), any(JdbcSourceTable.class),
                anyString(), anyDouble(), anyInt()))
                .thenReturn(boundaries);

        Collection<JdbcSourceSplit> splits = splitter.createSplits(basicJdbcSourceTable, getSplitKeyType());

        assertNotNull(splits);
        assertEquals(2, splits.size()); // n+1 splits for n boundaries

        List<JdbcSourceSplit> splitList = new ArrayList<>(splits);

        // 验证第一个分片：null to boundary[0]
        assertNull(splitList.get(0).getSplitStart());
        assertEquals(boundaries[0], splitList.get(0).getSplitEnd());
        assertEquals(SPLIT_KEY_NAME, splitList.get(0).getSplitKeyName());

        // 验证第二个分片：boundary[0] to null
        assertEquals(boundaries[0], splitList.get(1).getSplitStart());
        assertNull(splitList.get(1).getSplitEnd());
        assertEquals(SPLIT_KEY_NAME, splitList.get(1).getSplitKeyName());
    }

    /**
     * 测试目的：验证当采样返回多个边界值时，是否正确创建对应数量的分片
     * 期望：对于 n 个边界值，创建 n+1 个分片，每个分片的边界设置正确
     */
    @Test
    void testCreateSplits_multipleBoundaries() throws Exception {
        Object[] boundaries = new Object[]{100, 200, 300}; // 3 个边界值
        when(mockJdbcDialect.sampleAndCalculateBoundaries(
                any(Connection.class), any(JdbcSourceTable.class),
                anyString(), anyDouble(), anyInt()))
                .thenReturn(boundaries);

        Collection<JdbcSourceSplit> splits = splitter.createSplits(basicJdbcSourceTable, getSplitKeyType());

        assertNotNull(splits);
        assertEquals(4, splits.size()); // n+1 = 3+1 = 4 个分片

        List<JdbcSourceSplit> splitList = new ArrayList<>(splits);

        // 验证第一个分片：null to boundaries[0] (100)
        assertNull(splitList.get(0).getSplitStart());
        assertEquals(boundaries[0], splitList.get(0).getSplitEnd());

        // 验证第二个分片：boundaries[0] (100) to boundaries[1] (200)
        assertEquals(boundaries[0], splitList.get(1).getSplitStart());
        assertEquals(boundaries[1], splitList.get(1).getSplitEnd());

        // 验证第三个分片：boundaries[1] (200) to boundaries[2] (300)
        assertEquals(boundaries[1], splitList.get(2).getSplitStart());
        assertEquals(boundaries[2], splitList.get(2).getSplitEnd());

        // 验证第四个分片：boundaries[2] (300) to null
        assertEquals(boundaries[2], splitList.get(3).getSplitStart());
        assertNull(splitList.get(3).getSplitEnd());

        // 验证 Mock 方法调用
        verify(mockJdbcDialect, times(1)).sampleAndCalculateBoundaries(
                eq(mockConnection), eq(basicJdbcSourceTable), eq(SPLIT_KEY_NAME),
                anyDouble(), eq(DEFAULT_PARTITION_NUM));
    }

    // ==================== PreparedStatement 参数设置测试 ====================

    /**
     * 测试目的：验证首个分片的 PreparedStatement 参数设置是否正确
     * 期望：只设置一个参数（上边界），不设置下边界参数
     */
    @Test
    void testPrepareSampledBalancedSplitStatement_firstSplit() throws SQLException {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "s0", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, null, 100);

        splitter.prepareSampledBalancedSplitStatement(mockPreparedStatement, split);

        verify(mockPreparedStatement, times(1)).setObject(1, 100);
        verify(mockPreparedStatement, never()).setObject(eq(2), any());
    }

    /**
     * 测试目的：验证中间分片的 PreparedStatement 参数设置是否正确
     * 期望：设置两个参数，第一个参数为下边界，第二个参数为上边界
     */
    @Test
    void testPrepareSampledBalancedSplitStatement_middleSplit() throws SQLException {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "s1", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 100, 200);

        splitter.prepareSampledBalancedSplitStatement(mockPreparedStatement, split);

        verify(mockPreparedStatement, times(1)).setObject(1, 100); // 下边界 > ?
        verify(mockPreparedStatement, times(1)).setObject(2, 200); // 上边界 <= ?
    }

    /**
     * 测试目的：验证最后分片的 PreparedStatement 参数设置是否正确
     * 期望：只设置一个参数（下边界），不设置上边界参数
     */
    @Test
    void testPrepareSampledBalancedSplitStatement_lastSplit() throws SQLException {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "s2", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 200, null);

        splitter.prepareSampledBalancedSplitStatement(mockPreparedStatement, split);

        verify(mockPreparedStatement, times(1)).setObject(1, 200);
        verify(mockPreparedStatement, never()).setObject(eq(2), any());
    }

    /**
     * 测试目的：验证单个分片的 PreparedStatement 参数设置是否正确
     * 期望：不设置任何参数，因为没有 WHERE 条件
     */
    @Test
    void testPrepareSampledBalancedSplitStatement_singleSplit() throws SQLException {
        JdbcSourceSplit split = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "s0", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, null, null);

        splitter.prepareSampledBalancedSplitStatement(mockPreparedStatement, split);

        verify(mockPreparedStatement, never()).setObject(anyInt(), any());
    }

    // ==================== 集成测试 ====================

    /**
     * 测试目的：验证当分片包含分片键时，createSplitStatement 方法是否正确调用相关方法
     * 期望：正确生成 PreparedStatement 并设置参数
     */
    @Test
    void testCreateSplitStatement_callsCorrectMethods() throws SQLException {
        JdbcSourceSplit splitWithKey = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_1", null, SPLIT_KEY_NAME, SPLIT_KEY_TYPE, 100, 200);

        PreparedStatement resultStatement = splitter.createSplitStatement(splitWithKey, basicTableSchema);

        assertNotNull(resultStatement);
        assertEquals(mockPreparedStatement, resultStatement);

        // 验证参数设置（通过验证 setObject 调用来间接验证 prepareSampledBalancedSplitStatement 被调用）
        verify(mockPreparedStatement, times(1)).setObject(1, 100);
        verify(mockPreparedStatement, times(1)).setObject(2, 200);
    }

    /**
     * 测试目的：验证当分片不包含分片键时，createSplitStatement 方法是否调用单分片语句创建逻辑
     * 期望：调用 createSingleSplitStatement 方法，不设置任何参数
     */
    @Test
    void testCreateSplitStatement_noSplitKey_callsSingleSplitStatement() throws SQLException {
        JdbcSourceSplit splitNoKey = new JdbcSourceSplit(
                BASIC_TABLE_PATH, "split_0", "SELECT * FROM custom_table", null, null, null, null);

        PreparedStatement resultStatement = splitter.createSplitStatement(splitNoKey, basicTableSchema);

        assertNotNull(resultStatement);
        assertEquals(mockPreparedStatement, resultStatement);

        // 验证没有设置任何参数，因为这是一个没有边界的单分片
        verify(mockPreparedStatement, never()).setObject(anyInt(), any());
    }

    /**
     * 测试目的：验证采样均衡分片算法在数据高度倾斜场景下的行为
     * 期望：能够处理极端的边界值分布，正确创建分片
     */
    @Test
    void testCreateSplits_skewedDataDistribution() throws Exception {
        // 模拟数据倾斜：大部分数据集中在某几个区间
        Object[] skewedBoundaries = new Object[]{1, 2, 1000000};
        when(mockJdbcDialect.sampleAndCalculateBoundaries(
                any(Connection.class), any(JdbcSourceTable.class),
                anyString(), anyDouble(), anyInt()))
                .thenReturn(skewedBoundaries);

        Collection<JdbcSourceSplit> splits = splitter.createSplits(basicJdbcSourceTable, getSplitKeyType());

        assertNotNull(splits);
        assertEquals(4, splits.size());

        List<JdbcSourceSplit> splitList = new ArrayList<>(splits);

        // 验证分片能够正确处理倾斜的边界值
        assertEquals(1, splitList.get(0).getSplitEnd());
        assertEquals(1, splitList.get(1).getSplitStart());
        assertEquals(2, splitList.get(1).getSplitEnd());
        assertEquals(1000000, splitList.get(3).getSplitStart());
    }

    /**
     * 完整的采样参数与结果验证测试
     * @throws Exception
     */
    @Test
    void testCreateSplits_samplingParametersAndResultValidation() throws Exception {
        // 1. 设置采样配置
        when(mockJdbcSourceConfig.getSamplingPercentage()).thenReturn(0.001);

        // 2. 设置预期的采样结果
        Object[] expectedBoundaries = new Object[]{100, 200, 300};
        when(mockJdbcDialect.sampleAndCalculateBoundaries(
                any(Connection.class), any(JdbcSourceTable.class),
                anyString(), anyDouble(), anyInt()))
                .thenReturn(expectedBoundaries);

        // 3. 执行测试
        Collection<JdbcSourceSplit> actualSplits = splitter.createSplits(
                basicJdbcSourceTable, getSplitKeyType());

        // 4. 验证方法调用（过程验证）
        verify(mockJdbcDialect, times(1)).sampleAndCalculateBoundaries(
                eq(mockConnection),
                eq(basicJdbcSourceTable),
                eq(SPLIT_KEY_NAME),
                eq(0.001),  // 采样比例
                eq(DEFAULT_PARTITION_NUM));  // 分区数量

        // 🔥 5. 验证返回结果（结果验证）
        assertNotNull(actualSplits);
        assertEquals(4, actualSplits.size()); // 3个边界 = 4个分片

        List<JdbcSourceSplit> splitList = new ArrayList<>(actualSplits);

        // 验证第一个分片：[null, 100]
        assertEquals(null, splitList.get(0).getSplitStart());
        assertEquals(100, splitList.get(0).getSplitEnd());
        assertEquals(SPLIT_KEY_NAME, splitList.get(0).getSplitKeyName());
        assertEquals(SPLIT_KEY_TYPE, splitList.get(0).getSplitKeyType());
        assertTrue(splitList.get(0).getSplitId().contains(BASIC_TABLE_PATH.toString()));

        // 验证第二个分片：[100, 200]
        assertEquals(100, splitList.get(1).getSplitStart());
        assertEquals(200, splitList.get(1).getSplitEnd());

        // 验证第三个分片：[200, 300]
        assertEquals(200, splitList.get(2).getSplitStart());
        assertEquals(300, splitList.get(2).getSplitEnd());

        // 验证第四个分片：[300, null]
        assertEquals(300, splitList.get(3).getSplitStart());
        assertEquals(null, splitList.get(3).getSplitEnd());

        // 验证所有分片的ID唯一性
        Set<String> splitIds = actualSplits.stream()
                .map(JdbcSourceSplit::getSplitId)
                .collect(Collectors.toSet());
        assertEquals(4, splitIds.size()); // 确保没有重复ID
    }
}
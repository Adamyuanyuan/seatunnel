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

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 采样均衡分片器
 *
 * 算法原理：
 * 采样均衡分片（Sampled Balanced Sharding）算法以数据分布为导向，通过以下步骤实现更均衡的数据分片：
 *
 * 1. 数据采样：从目标表随机抽取一定比例（如千分之一）的数据作为样本，以低成本获取数据分布特征
 * 2. 分位数计算：使用NTILE窗口函数将采样数据按照指定的分片数量分成等量的桶，每个桶包含大致相同数量的样本记录
 * 3. 边界确定：计算每个桶的最大值，作为实际分片的分割边界
 * 4. 均衡分割：基于实际数据分布而非简单的数值区间均分，确保每个分片处理大致相同数量的记录
 *
 * 适用场景：
 * - 数据极度倾斜，如ID分布存在大段空洞或聚集区域
 * - 并行处理任务负载不均衡，部分任务过重而其它任务几乎空闲
 * - 需要优化资源利用率，提高整体处理效率
 */


/**
 * Sampled Balanced Chunk Splitter
 *
 * Algorithm Principles:
 * The Sampled Balanced Sharding algorithm is data distribution-oriented and implements
 * more balanced data partitioning through the following steps:
 *
 * 1. Data Sampling: Randomly extracts a certain percentage (e.g., 0.1%) of data from
 *    the target table as samples to obtain data distribution characteristics at low cost
 * 2. Quantile Calculation: Uses NTILE window function to divide sampled data into
 *    equal-sized buckets based on specified partition count, each bucket containing
 *    approximately the same number of sample records
 * 3. Boundary Determination: Calculates the maximum value of each bucket as the actual
 *    partition boundary
 * 4. Balanced Partitioning: Based on actual data distribution rather than simple
 *    numerical range division, ensuring each partition processes approximately the
 *    same number of records
 *
 * Applicable Scenarios:
 * - Severely skewed data where ID distribution has large gaps or concentrated areas
 * - Parallel processing tasks with unbalanced workloads where some tasks are overloaded
 *   while others are nearly idle
 * - Scenarios requiring optimized resource utilization and improved overall processing efficiency
 */
@Slf4j
public class SampledBalancedChunkSplitter extends ChunkSplitter {

    public SampledBalancedChunkSplitter(JdbcSourceConfig config) {
        super(config);
    }

    @Override
    protected Collection<JdbcSourceSplit> createSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKeyType) throws Exception {
        return createSampledBalancedSplits(table, splitKeyType);
    }

    @Override
    protected PreparedStatement createSplitStatement(
            JdbcSourceSplit split, TableSchema schema) throws SQLException {
        return createSampledBalancedSplitStatement(split, schema);
    }

    private Collection<JdbcSourceSplit> createSampledBalancedSplits(
            JdbcSourceTable table, SeaTunnelRowType splitKey) throws Exception {

        String splitKeyName = splitKey.getFieldNames()[0];
        SeaTunnelDataType<?> splitKeyType = splitKey.getFieldType(0);

        log.info("Using sampled balanced sharding strategy for table {} with split column: {}",
                table.getTablePath(), splitKeyName);
        log.info("config: {}", config);

        // 内部计算时使用 bucket_number - 1 作为桶数
//        int actualBuckets = config.getBucketNumber() - 1;

        // 1. Execute data sampling and quantile calculation
        Object[] boundaries = jdbcDialect.sampleAndCalculateBoundaries(
                getOrEstablishConnection(),
                table,
                splitKeyName,
                config.getSamplingPercentage(),
                config.getBucketNumber()
        );

        if (boundaries.length == 0) {
            log.warn("No valid boundaries found from sampling, falling back to single split");
            return Collections.singletonList(createSingleSplit(table));
        }

        log.info("Sampling yielded {} boundary values, will generate {} splits",
                boundaries.length, boundaries.length + 1);

        // 2. Generate splits based on boundaries
        List<JdbcSourceSplit> splits = new ArrayList<>();
        Object previousBoundary = null;

        // Generate n+1 splits for n boundary values
        for (int i = 0; i <= boundaries.length; i++) {
            Object currentBoundary = (i < boundaries.length) ? boundaries[i] : null;

            JdbcSourceSplit split = new JdbcSourceSplit(
                    table.getTablePath(),
                    createSplitId(table.getTablePath(), i),
                    table.getQuery(),
                    splitKeyName,
                    splitKeyType,
                    previousBoundary,  // Split start value (null for first split)
                    currentBoundary);  // Split end value (null for last split)

            splits.add(split);

            log.info("Created split {}: {} < {} <= {}",
                    i, previousBoundary, splitKeyName, currentBoundary);

            previousBoundary = currentBoundary;
        }

        log.info("Sampled balanced sharding completed, generated {} splits", splits.size());
        return splits;
    }

    private PreparedStatement createSampledBalancedSplitStatement(
            JdbcSourceSplit split, TableSchema schema) throws SQLException {

        // If no split key, use single split query
        if (split.getSplitKeyName() == null) {
            return createSingleSplitStatement(split);
        }

        // Generate query statement with conditions
        String splitQuery = createSampledBalancedSplitQuerySQL(split, schema);
        PreparedStatement statement = createPreparedStatement(splitQuery);
        prepareSampledBalancedSplitStatement(statement, split);
        return statement;
    }

    @VisibleForTesting
    String createSampledBalancedSplitQuerySQL(JdbcSourceSplit split, TableSchema schema) {
        String splitKeyName = jdbcDialect.quoteIdentifier(split.getSplitKeyName());
        boolean isFirstSplit = split.getSplitStart() == null;
        boolean isLastSplit = split.getSplitEnd() == null;

        String baseQuery;
        if (StringUtils.isNotBlank(split.getSplitQuery())) {
            baseQuery = String.format("SELECT * FROM (%s) tmp", split.getSplitQuery());
        } else {
            baseQuery = String.format("SELECT * FROM %s",
                    jdbcDialect.tableIdentifier(split.getTablePath()));
        }

        // Generate different WHERE conditions based on split position
        if (isFirstSplit && isLastSplit) {
            // Only one split, no WHERE condition needed
            return baseQuery;
        } else if (isFirstSplit) {
            // First split: WHERE column <= ?
            return String.format("%s WHERE %s <= ?", baseQuery, splitKeyName);
        } else if (isLastSplit) {
            // Last split: WHERE column > ?
            return String.format("%s WHERE %s > ?", baseQuery, splitKeyName);
        } else {
            // Middle split: WHERE column > ? AND column <= ?
            return String.format("%s WHERE %s > ? AND %s <= ?",
                    baseQuery, splitKeyName, splitKeyName);
        }
    }

    @VisibleForTesting
    void prepareSampledBalancedSplitStatement(
            PreparedStatement statement, JdbcSourceSplit split) throws SQLException {

        boolean isFirstSplit = split.getSplitStart() == null;
        boolean isLastSplit = split.getSplitEnd() == null;

        if (isFirstSplit && isLastSplit) {
            // Single split, no parameters needed
            return;
        }

        int paramIndex = 1;

        // Set lower bound parameter (except for first split)
        if (!isFirstSplit) {
            statement.setObject(paramIndex++, split.getSplitStart());
        }

        // Set upper bound parameter (except for last split)
        if (!isLastSplit) {
            statement.setObject(paramIndex, split.getSplitEnd());
        }

        log.debug("Split parameters set - start value: {}, end value: {}",
                split.getSplitStart(), split.getSplitEnd());
    }
}
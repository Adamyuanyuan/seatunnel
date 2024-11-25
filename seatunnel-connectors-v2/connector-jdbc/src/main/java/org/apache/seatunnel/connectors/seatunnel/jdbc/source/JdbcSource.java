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

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sourcetype.DatabaseTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);
    private static final String POINT = ".";
    private final JdbcSourceConfig jdbcSourceConfig;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    @SneakyThrows
    public JdbcSource(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        JdbcConnectionConfig jdbcConnectionConfig = jdbcSourceConfig.getJdbcConnectionConfig();
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcConnectionConfig.getUrl(), jdbcConnectionConfig.getCompatibleMode());
        JdbcConnectionProvider connectionProvider =
                jdbcDialect.getJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
        Connection connection = connectionProvider.getOrEstablishConnection();
        List<JdbcSourceTableConfig> jdbcSourceTableConfigs = new ArrayList<>();
        ResultSet rs = null;
        PreparedStatement ps = null;
        boolean containsInstances = true;
        List<JdbcSourceTableConfig> tablePaths = jdbcSourceConfig.getTableConfigList();
        try {
            for (JdbcSourceTableConfig tableConfig : tablePaths) {
                List<String> schemaTables = new ArrayList<>();
                String tablePath = tableConfig.getTablePath();
                String query = tableConfig.getQuery();
                String sql;
                if (StringUtils.isBlank(query)) {
                    String schemaName;
                    if (jdbcDialect.dialectName().startsWith(DatabaseTypeEnum.ORACLE.getValue())) {
                        schemaName = tablePath.split("\\.")[1];
                        sql = "SELECT OWNER, TABLE_NAME FROM dba_tables where OWNER=?";
                    } else if (jdbcDialect
                            .dialectName()
                            .equals(DatabaseTypeEnum.MYSQL.getValue())) {
                        containsInstances = false;
                        schemaName = tablePath.split("\\.")[0];
                        sql =
                                "SELECT TABLE_SCHEMA, TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?";
                    } else if (jdbcDialect
                            .dialectName()
                            .equals(DatabaseTypeEnum.SQLSERVER.getValue())) {
                        schemaName = tablePath.split("\\.")[1];
                        sql =
                                "SELECT TABLE_SCHEMA, TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?";
                    } else {
                        throw new RuntimeException(
                                "not support dialect " + jdbcDialect.dialectName());
                    }
                    ps = connection.prepareStatement(sql);
                    ps.setString(1, schemaName);
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        schemaTables.add(rs.getString(1) + POINT + rs.getString(2));
                    }
                    filterCapturedTablesByRegrex(
                            jdbcSourceTableConfigs,
                            containsInstances,
                            tableConfig,
                            schemaTables,
                            tablePath);
                } else {
                    jdbcSourceTableConfigs.add(tableConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Regular expression match failed:", e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            connectionProvider.closeConnection();
        }

        this.jdbcSourceTables =
                JdbcCatalogUtils.getTables(
                        jdbcSourceConfig.getJdbcConnectionConfig(), jdbcSourceTableConfigs);
    }

    private void filterCapturedTablesByRegrex(
            List<JdbcSourceTableConfig> jdbcSourceTableConfigs,
            boolean containsInstances,
            JdbcSourceTableConfig tableConfig,
            List<String> schemaTables,
            String tablePath) {
        Pattern pattern = Pattern.compile(tablePath);
        String[] tablePathSplits = tablePath.split("\\.");
        String tableName =
                containsInstances
                        ? tablePathSplits[1] + POINT + tablePathSplits[2]
                        : tablePathSplits[0] + POINT + tablePathSplits[1];
        if (schemaTables.contains(tableName)) {
            JdbcSourceTableConfig jdbcSourceTableConfig = new JdbcSourceTableConfig();
            jdbcSourceTableConfig.setTablePath(tableName);
            jdbcSourceTableConfigs.add(tableConfig);
        } else {
            for (String table : schemaTables) {
                Matcher matcher =
                        pattern.matcher(
                                containsInstances ? tablePathSplits[0] + POINT + table : table);
                while (matcher.find()) {
                    LOG.info("found regrex match table: {}", table);
                    JdbcSourceTableConfig jdbcSourceTableConfig = new JdbcSourceTableConfig();
                    jdbcSourceTableConfig.setTablePath(table);
                    jdbcSourceTableConfigs.add(jdbcSourceTableConfig);
                }
            }
        }
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return jdbcSourceTables.values().stream()
                .map(JdbcSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        Map<TablePath, CatalogTable> tables = new HashMap<>();
        for (TablePath tablePath : jdbcSourceTables.keySet()) {
            tables.put(tablePath, jdbcSourceTables.get(tablePath).getCatalogTable());
        }
        return new JdbcSourceReader(readerContext, jdbcSourceConfig, tables);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, null);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceState checkpointState)
            throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, checkpointState);
    }
}

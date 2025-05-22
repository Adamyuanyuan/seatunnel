/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.doris.schema;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV2;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisSchemaChangeException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SchemaChangeManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String CHECK_COLUMN_EXISTS =
            "SELECT COLUMN_NAME FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private static final String CHECK_TABLE_EXISTS =
            "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";
    private ObjectMapper objectMapper = new ObjectMapper();
    private DorisSinkConfig dorisSinkConfig;
    private String charsetEncoding = "UTF-8";

    public SchemaChangeManager(DorisSinkConfig dorisSinkConfig) {
        this.dorisSinkConfig = dorisSinkConfig;
    }

    public SchemaChangeManager(DorisSinkConfig dorisSinkConfig, String charsetEncoding) {
        this.dorisSinkConfig = dorisSinkConfig;
        this.charsetEncoding = charsetEncoding;
    }

    /**
     * Refresh physical table schema by schema change event
     *
     * @param event schema change event
     * @param tablePath sink table path
     */
    public void applySchemaChange(TablePath tablePath, SchemaChangeEvent event) throws IOException {
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySchemaChange(tablePath, columnEvent);
            }
        } else {
            if (event instanceof AlterTableChangeColumnEvent) {
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                if (!changeColumnEvent
                        .getOldColumn()
                        .equals(changeColumnEvent.getColumn().getName())) {
                    if (!columnExists(tablePath, changeColumnEvent.getOldColumn())
                            && columnExists(tablePath, changeColumnEvent.getColumn().getName())) {
                        log.warn(
                                "Column {} already exists in table {}. Skipping change column operation. event: {}",
                                changeColumnEvent.getColumn().getName(),
                                tablePath.getFullName(),
                                event);
                        return;
                    }
                }
                applySchemaChange(tablePath, changeColumnEvent);
            } else if (event instanceof AlterTableModifyColumnEvent) {
                applySchemaChange(tablePath, (AlterTableModifyColumnEvent) event);
            } else if (event instanceof AlterTableAddColumnEvent) {
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                if (columnExists(tablePath, addColumnEvent.getColumn().getName())) {
                    log.warn(
                            "Column {} already exists in table {}. Skipping add column operation. event: {}",
                            addColumnEvent.getColumn().getName(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(tablePath, addColumnEvent);
            } else if (event instanceof AlterTableDropColumnEvent) {
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                if (!columnExists(tablePath, dropColumnEvent.getColumn())) {
                    log.warn(
                            "Column {} does not exist in table {}. Skipping drop column operation. event: {}",
                            dropColumnEvent.getColumn(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(tablePath, dropColumnEvent);
            } else {
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent : " + event.getEventType());
            }
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableChangeColumnEvent event)
            throws IOException {
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("RENAME COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getOldColumn()))
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()));
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String changeColumnSQL = sqlBuilder.toString();
        if (!execute(changeColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table change column, SQL:" + changeColumnSQL);
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableModifyColumnEvent event)
            throws IOException {
        BasicTypeDefine typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(event.getColumn());
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("MODIFY COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String modifyColumnSQL = sqlBuilder.toString();
        if (!execute(modifyColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table modify column, SQL:" + modifyColumnSQL);
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableAddColumnEvent event)
            throws IOException {
        BasicTypeDefine typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(event.getColumn());
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("ADD COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());
        if (event.getColumn().getDefaultValue() != null
                && isSupportDefaultValue(event.getColumn())) {
            sqlBuilder
                    .append(" DEFAULT ")
                    .append(quoteDefaultValue(event.getColumn().getDefaultValue()));
        }
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String addColumnSQL = sqlBuilder.toString();
        if (!execute(addColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table add column, SQL:" + addColumnSQL);
        }
    }

    /**
     * Support Default Value
     *
     * @param column
     * @return
     */
    // todo support more type
    private boolean isSupportDefaultValue(Column column) {
        switch (column.getDataType().getSqlType()) {
            case STRING:
            case BIGINT:
            case INT:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableDropColumnEvent event)
            throws IOException {
        String dropColumnSQL =
                String.format(
                        "ALTER TABLE %s DROP COLUMN %s",
                        tablePath.getFullName(), quoteIdentifier(event.getColumn()));
        if (!execute(dropColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table drop column, SQL:" + dropColumnSQL);
        }
    }

    /** execute sql in doris. */
    public boolean execute(String ddl, String database)
            throws IOException, IllegalArgumentException {
        String responseEntity = executeThenReturnResponse(ddl, database);
        return handleSchemaChange(responseEntity);
    }

    private String executeThenReturnResponse(String ddl, String database)
            throws IOException, IllegalArgumentException {
        if (StringUtils.isEmpty(ddl)) {
            throw new IllegalArgumentException("ddl can not be null or empty string!");
        }
        log.info("Execute SQL: {}", ddl);
        HttpPost httpPost = buildHttpPost(ddl, database);
        return handleResponse(httpPost);
    }

    private boolean handleSchemaChange(String responseEntity) throws JsonProcessingException {
        Map<String, Object> responseMap = objectMapper.readValue(responseEntity, Map.class);
        String code = responseMap.getOrDefault("code", "-1").toString();
        if (code.equals("0")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the column exists in the table
     *
     * @param tablePath
     * @param column
     * @return
     */
    public boolean columnExists(TablePath tablePath, String column) throws IOException {
        // 处理列名大小写
        if (dorisSinkConfig != null && !dorisSinkConfig.isCaseSensitive()) {
            column = column.toLowerCase();
        }

        String selectColumnSQL =
                buildColumnExistsQuery(
                        tablePath.getDatabaseName(), tablePath.getTableName(), column);
        return sendCheckColumnHttpPostRequest(selectColumnSQL, tablePath.getDatabaseName());
    }

    public static String buildColumnExistsQuery(String database, String table, String column) {
        return String.format(CHECK_COLUMN_EXISTS, database, table, column);
    }

    /**
     * Check if the table exists in the database
     *
     * @param tablePath 表路径
     * @return 表是否存在
     */
    public boolean tableExists(TablePath tablePath) throws IOException {
        String database = tablePath.getDatabaseName();
        String table = tablePath.getTableName();

        String checkTableSQL = String.format(CHECK_TABLE_EXISTS, database, table);
        return sendCheckTableHttpPostRequest(checkTableSQL, database);
    }

    private boolean sendCheckTableHttpPostRequest(String sql, String database)
            throws IOException, IllegalArgumentException {
        HttpPost httpPost = buildHttpPost(sql, database);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(httpPost);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                log.info(
                        "http post response success. statusCode: {}, loadResult: {}",
                        statusCode,
                        loadResult);
                JsonNode responseNode = objectMapper.readTree(loadResult);
                String code = responseNode.get("code").asText("-1");
                if (code.equals("0")) {
                    JsonNode data = responseNode.get("data").get("data");
                    if (!data.isEmpty()) {
                        return true;
                    }
                }
            } else {
                log.warn("http post response failed. statusCode: {}", statusCode);
            }
        } catch (Exception e) {
            log.error(
                    "send http post request error {}, default return false, SQL:{}",
                    e.getMessage(),
                    sql);
            log.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * 创建表
     *
     * @param tablePath 表路径
     * @param tableSchema 表结构
     */
    public void createTable(
            TablePath tablePath, org.apache.seatunnel.api.table.catalog.TableSchema tableSchema)
            throws IOException {
        // 定义默认的主键表模板
        String defaultTemplate =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                        + "${rowtype_primary_key},\n"
                        + "${rowtype_fields}\n"
                        + ") ENGINE=OLAP\n"
                        + " UNIQUE KEY (${rowtype_primary_key})\n"
                        + "DISTRIBUTED BY HASH (${rowtype_primary_key})\n"
                        + " PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"in_memory\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"disable_auto_compaction\" = \"false\"\n"
                        + ")";

        String createTableTemplate;

        log.info(
                "Create table template from config: {}",
                dorisSinkConfig.getCreateTableTemplate() != null
                        ? dorisSinkConfig.getCreateTableTemplate()
                        : "null");
        // 判断是否设置了自定义模板
        if (dorisSinkConfig.getCreateTableTemplate() != null
                && !dorisSinkConfig.getCreateTableTemplate().isEmpty()
                && !dorisSinkConfig.getCreateTableTemplate().equals(defaultTemplate)) {
            createTableTemplate = dorisSinkConfig.getCreateTableTemplate();
            log.info("Using custom table template from config");
        }
        // 判断是否有主键
        else if (tableSchema.getPrimaryKey() != null
                && !tableSchema.getPrimaryKey().getColumnNames().isEmpty()) {
            List<String> primaryKeys =
                    new ArrayList<>(tableSchema.getPrimaryKey().getColumnNames());
            log.info("Using primary key from table schema: {}", String.join(", ", primaryKeys));
            // 有主键，使用默认主键表模板
            createTableTemplate = defaultTemplate;
        }
        // 无主键，使用明细表模板
        else {
            // 确定默认键列
            String defaultKeyColumn = "__doris_default_key__";
            if (!tableSchema.getColumns().isEmpty()) {
                Column firstColumn = tableSchema.getColumns().get(0);
                String firstColumnName = firstColumn.getName();
                if (dorisSinkConfig != null && !dorisSinkConfig.isCaseSensitive()) {
                    firstColumnName = firstColumnName.toLowerCase();
                }
                defaultKeyColumn = quoteIdentifier(firstColumnName);
            }

            log.info("No primary key found, using DUPLICATE KEY with column: {}", defaultKeyColumn);

            // 无主键，使用明细表模板
            createTableTemplate =
                    "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                            + "${rowtype_fields}\n"
                            + ") ENGINE=OLAP\n"
                            + " DUPLICATE KEY ("
                            + defaultKeyColumn
                            + ")\n"
                            + "DISTRIBUTED BY HASH ("
                            + defaultKeyColumn
                            + ") BUCKETS 10\n"
                            + " PROPERTIES (\n"
                            + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                            + "\"in_memory\" = \"false\",\n"
                            + "\"storage_format\" = \"V2\",\n"
                            + "\"disable_auto_compaction\" = \"false\"\n"
                            + ")";
        }

        log.info("Creating table with template: {}", createTableTemplate);

        // 构建列定义
        StringBuilder primaryKeyColumns = new StringBuilder();
        StringBuilder normalColumns = new StringBuilder();
        StringBuilder primaryKeyNames = new StringBuilder();

        // 获取主键列
        List<String> primaryKeys = new ArrayList<>();
        if (tableSchema.getPrimaryKey() != null
                && !tableSchema.getPrimaryKey().getColumnNames().isEmpty()) {
            primaryKeys.addAll(tableSchema.getPrimaryKey().getColumnNames());
        }

        // 处理所有列
        for (Column column : tableSchema.getColumns()) {
            BasicTypeDefine typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(column);

            // 处理列名大小写
            String columnName = column.getName();
            if (dorisSinkConfig != null && !dorisSinkConfig.isCaseSensitive()) {
                columnName = columnName.toLowerCase();
            }

            // 构建列定义
            StringBuilder columnDef = new StringBuilder();
            columnDef
                    .append(quoteIdentifier(columnName))
                    .append(" ")
                    .append(typeDefine.getColumnType());

            if (!column.isNullable()) {
                columnDef.append(" NOT NULL");
            }

            if (column.getDefaultValue() != null && isSupportDefaultValue(column)) {
                columnDef.append(" DEFAULT ").append(quoteDefaultValue(column.getDefaultValue()));
            }

            if (column.getComment() != null) {
                columnDef.append(" COMMENT '").append(column.getComment()).append("'");
            }

            // 判断是否为主键列
            boolean isPrimaryKey =
                    primaryKeys.stream()
                            .anyMatch(
                                    pk ->
                                            dorisSinkConfig != null
                                                            && !dorisSinkConfig.isCaseSensitive()
                                                    ? column.getName().equalsIgnoreCase(pk)
                                                    : column.getName().equals(pk));

            // 添加到相应的列集合
            if (isPrimaryKey) {
                if (primaryKeyColumns.length() > 0) primaryKeyColumns.append(", ");
                primaryKeyColumns.append(columnDef);

                if (primaryKeyNames.length() > 0) primaryKeyNames.append(", ");
                primaryKeyNames.append(quoteIdentifier(columnName));
            } else {
                if (normalColumns.length() > 0) normalColumns.append(", ");
                normalColumns.append(columnDef);
            }
        }

        // 替换模板中的占位符
        String createTableSql = createTableTemplate;
        createTableSql =
                createTableSql
                        .replace("${database}", tablePath.getDatabaseName())
                        .replace("${table}", tablePath.getTableName())
                        .replace("${table_name}", tablePath.getTableName());

        // 替换列定义相关占位符
        boolean hasPrimaryKey = primaryKeyColumns.length() > 0;

        if (hasPrimaryKey) {
            // 替换主键列定义（在列定义部分）
            createTableSql =
                    createTableSql.replace(
                            "${rowtype_primary_key},", primaryKeyColumns.toString() + ",");

            // 替换主键列名（用于 UNIQUE KEY 和 DISTRIBUTED BY HASH）
            createTableSql =
                    createTableSql.replace(
                            "UNIQUE KEY (${rowtype_primary_key})",
                            "UNIQUE KEY (" + primaryKeyNames.toString() + ")");
            createTableSql =
                    createTableSql.replace(
                            "DISTRIBUTED BY HASH (${rowtype_primary_key})",
                            "DISTRIBUTED BY HASH (" + primaryKeyNames.toString() + ")");
        } else {
            // 无主键情况下，删除主键相关占位符
            createTableSql = createTableSql.replace("${rowtype_primary_key},", "");
        }

        // 替换普通列定义
        createTableSql = createTableSql.replace("${rowtype_fields}", normalColumns.toString());

        log.info("Execute SQL: {}", createTableSql);

        // 执行创建表SQL
        if (!execute(createTableSql, tablePath.getDatabaseName())) {
            throw new DorisSchemaChangeException(
                    DorisConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    "Failed to create table: " + tablePath.getFullName());
        }

        log.info("Table {} created successfully", tablePath.getFullName());
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public static String quoteDefaultValue(Object defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.toString().startsWith("current_timestamp")) {
            return "current_timestamp";
        }
        return "'" + defaultValue + "'";
    }

    private boolean sendCheckColumnHttpPostRequest(String sql, String database)
            throws IOException, IllegalArgumentException {
        HttpPost httpPost = buildHttpPost(sql, database);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(httpPost);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                log.info(
                        "http post response success. statusCode: {}, loadResult: {}",
                        statusCode,
                        loadResult);
                JsonNode responseNode = objectMapper.readTree(loadResult);
                String code = responseNode.get("code").asText("-1");
                if (code.equals("0")) {
                    JsonNode data = responseNode.get("data").get("data");
                    if (!data.isEmpty()) {
                        return true;
                    }
                }
            } else {
                log.warn("http post response failed. statusCode: {}", statusCode);
            }
        } catch (Exception e) {
            log.error(
                    "send http post request error {}, default return false, SQL:{}",
                    e.getMessage(),
                    sql);
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public HttpPost buildHttpPost(String ddl, String database)
            throws IllegalArgumentException, IOException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", ddl);
        List<String> feNodes = Arrays.asList(dorisSinkConfig.getFrontends().split(","));
        Collections.shuffle(feNodes);
        String requestUrl = String.format(SCHEMA_CHANGE_API, feNodes.get(0), database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(
                HttpHeaders.CONTENT_TYPE,
                String.format("application/json;charset=%s", charsetEncoding));
        httpPost.setEntity(
                new StringEntity(objectMapper.writeValueAsString(param), charsetEncoding));
        return httpPost;
    }

    private String handleResponse(HttpUriRequest request) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                log.info(
                        "http post response success. statusCode: {}, loadResult: {}",
                        statusCode,
                        loadResult);
                return loadResult;
            } else {
                throw new DorisSchemaChangeException(
                        DorisConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "Failed to schemaChange, status: "
                                + statusCode
                                + ", reason: "
                                + reasonPhrase);
            }
        } catch (Exception e) {
            log.error("SchemaChange request error,", e);
            throw new DorisSchemaChangeException(
                    DorisConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    "SchemaChange request error with " + e.getMessage());
        }
    }

    private String authHeader() {
        return "Basic "
                + new String(
                        Base64.encodeBase64(
                                (dorisSinkConfig.getUsername()
                                                + ":"
                                                + dorisSinkConfig.getPassword())
                                        .getBytes(StandardCharsets.UTF_8)));
    }
}

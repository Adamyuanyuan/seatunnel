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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.doris.config.DorisSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.common.sql.template.SqlTemplate;
import org.apache.seatunnel.connectors.seatunnel.common.util.CreateTableParser;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DorisCatalogUtil {

    public static final String ALL_DATABASES_QUERY =
            "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE CATALOG_NAME = 'internal' ORDER BY SCHEMA_NAME";

    public static final String DATABASE_QUERY =
            "SELECT SCHEMA_NAME FROM information_schema.schemata "
                    + "WHERE CATALOG_NAME = 'internal' AND SCHEMA_NAME = ? "
                    + "ORDER BY SCHEMA_NAME";

    public static final String TABLES_QUERY_WITH_DATABASE_QUERY =
            "SELECT TABLE_NAME FROM information_schema.tables "
                    + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? "
                    + "ORDER BY TABLE_NAME";

    public static final String TABLES_QUERY_WITH_IDENTIFIER_QUERY =
            "SELECT TABLE_NAME FROM information_schema.tables "
                    + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "ORDER BY TABLE_NAME";

    public static final String TABLE_SCHEMA_QUERY =
            "SELECT * "
                    + "FROM information_schema.columns "
                    + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "ORDER BY ORDINAL_POSITION";

    public static final String QUERY_DORIS_VERSION_QUERY =
            "show variables like \"version_comment\";";

    public static String randomFrontEndHost(String[] frontEndNodes) {
        if (frontEndNodes.length == 1) {
            return frontEndNodes[0].split(":")[0];
        }
        List<String> list = Arrays.asList(frontEndNodes);
        Collections.shuffle(list);
        return list.get(0).split(":")[0];
    }

    public static String getJdbcUrl(String host, Integer port, String database) {
        return String.format("jdbc:mysql://%s:%d/%s", host, port, database);
    }

    public static String getCreateDatabaseQuery(String database, boolean ignoreIfExists) {
        return "CREATE DATABASE " + (ignoreIfExists ? "IF NOT EXISTS " : "") + database;
    }

    public static String getDropDatabaseQuery(String database, boolean ignoreIfNotExists) {
        return "DROP DATABASE " + (ignoreIfNotExists ? "IF EXISTS " : "") + database;
    }

    public static String getDropTableQuery(TablePath tablePath, boolean ignoreIfNotExists) {
        return "DROP TABLE " + (ignoreIfNotExists ? "IF EXISTS " : "") + tablePath.getFullName();
    }

    public static String getTruncateTableQuery(TablePath tablePath) {
        return "TRUNCATE TABLE " + tablePath.getFullName();
    }

    /**
     * @param createTableTemplate create table template
     * @param catalogTable catalog table
     * @param typeConverter
     * @return create table stmt
     */
    public static String getCreateTableStatement(
            String createTableTemplate,
            TablePath tablePath,
            CatalogTable catalogTable,
            TypeConverter<BasicTypeDefine> typeConverter,
            boolean caseSensitive) {

        String template = createTableTemplate;
        TableSchema tableSchema = catalogTable.getTableSchema();

        // 处理数据库名和表名的大小写
        String dbName =
                caseSensitive
                        ? tablePath.getDatabaseName()
                        : tablePath.getDatabaseName().toLowerCase();
        String tbName =
                caseSensitive ? tablePath.getTableName() : tablePath.getTableName().toLowerCase();

        String primaryKey = "";
        boolean hasPrimaryKey = false;
        if (tableSchema.getPrimaryKey() != null
                && !tableSchema.getPrimaryKey().getColumnNames().isEmpty()) {
            hasPrimaryKey = true;
            primaryKey =
                    tableSchema.getPrimaryKey().getColumnNames().stream()
                            .map(
                                    r -> {
                                        String colName = caseSensitive ? r : r.toLowerCase();
                                        return "`" + colName + "`";
                                    })
                            .collect(Collectors.joining(","));
        }

        // 如果没有主键，并且模板中包含主键占位符，则使用明细表模板
        if (!hasPrimaryKey
                && template.contains(SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder())) {
            // 获取第一列作为默认键
            String defaultKeyColumn = "__doris_default_key__";
            if (!tableSchema.getColumns().isEmpty()) {
                Column firstColumn = tableSchema.getColumns().get(0);
                String firstColumnName = firstColumn.getName();
                if (!caseSensitive) {
                    firstColumnName = firstColumnName.toLowerCase();
                }
                defaultKeyColumn = "`" + firstColumnName + "`";
            }

            // 使用明细表模板
            template =
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

            log.info(
                    "Using duplicate key template for table without primary key: {}",
                    tablePath.getFullName());
        }

        String uniqueKey = "";
        if (!tableSchema.getConstraintKeys().isEmpty()) {
            uniqueKey =
                    tableSchema.getConstraintKeys().stream()
                            .flatMap(c -> c.getColumnNames().stream())
                            .map(
                                    r -> {
                                        String colName =
                                                caseSensitive
                                                        ? r.getColumnName()
                                                        : r.getColumnName().toLowerCase();
                                        return "`" + colName + "`";
                                    })
                            .collect(Collectors.joining(","));
        }

        // dup key
        String dupKey = "";
        if (catalogTable.getOptions() != null
                && StringUtils.isNotBlank(
                        catalogTable
                                .getOptions()
                                .get(
                                        SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY
                                                .getPlaceHolderKey()))) {
            String dupKeyColumns =
                    catalogTable
                            .getOptions()
                            .get(SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getPlaceHolderKey());
            dupKey =
                    Arrays.stream(dupKeyColumns.split(","))
                            .map(
                                    r -> {
                                        String colName = caseSensitive ? r : r.toLowerCase();
                                        return "`" + colName + "`";
                                    })
                            .collect(Collectors.joining(","));
        }

        // 只有当模板中包含主键占位符且有主键时才进行替换检查
        if (template.contains(SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder())) {
            if (hasPrimaryKey) {
                template =
                        template.replaceAll(
                                SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getReplacePlaceHolder(),
                                primaryKey);
            } else {
                // 如果没有主键，直接移除主键相关占位符
                template =
                        template.replaceAll(
                                SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getReplacePlaceHolder()
                                        + ",",
                                "");
                template =
                        template.replaceAll(
                                SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getReplacePlaceHolder(),
                                "");
            }
        }

        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getPlaceHolder(),
                uniqueKey,
                tablePath.getFullName(),
                DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getReplacePlaceHolder(), uniqueKey);
        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getPlaceHolder(),
                dupKey,
                tablePath.getFullName(),
                DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getReplacePlaceHolder(), dupKey);

        // 处理表注释占位符
        if (template.contains("COMMENT '${comment}'")) {
            String tableComment = "";
            if (catalogTable.getOptions() != null
                    && catalogTable.getOptions().containsKey("comment")) {
                tableComment = catalogTable.getOptions().get("comment");
            }
            template =
                    template.replace(
                            "COMMENT '${comment}'",
                            StringUtils.isNotBlank(tableComment)
                                    ? "COMMENT '" + tableComment + "'"
                                    : "");
        }

        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template =
                mergeColumnInTemplate(
                        columnInTemplate, tableSchema, template, typeConverter, caseSensitive);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(
                                column -> {
                                    String columnName =
                                            caseSensitive
                                                    ? column.getName()
                                                    : column.getName().toLowerCase();
                                    return !columnInTemplate.containsKey(columnName);
                                })
                        .map(x -> columnToDorisType(x, typeConverter, caseSensitive))
                        .collect(Collectors.joining(",\n"));

        if (template.contains(SaveModePlaceHolder.TABLE_NAME.getPlaceHolder())) {
            // TODO: Remove this compatibility config
            template =
                    template.replaceAll(
                            SaveModePlaceHolder.TABLE_NAME.getReplacePlaceHolder(), tbName);
            log.warn(
                    "The variable placeholder `${table_name}` has been marked as deprecated and will be removed soon, please use `${table}`");
        }

        return template.replaceAll(SaveModePlaceHolder.DATABASE.getReplacePlaceHolder(), dbName)
                .replaceAll(SaveModePlaceHolder.TABLE.getReplacePlaceHolder(), tbName)
                .replaceAll(
                        SaveModePlaceHolder.ROWTYPE_FIELDS.getReplacePlaceHolder(), rowTypeFields);
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template,
            TypeConverter<BasicTypeDefine> typeConverter,
            boolean caseSensitive) {
        int offset = 0;
        Map<String, Column> columnMap = new HashMap<>();
        for (Column column : tableSchema.getColumns()) {
            String key = caseSensitive ? column.getName() : column.getName().toLowerCase();
            columnMap.put(key, column);
        }
        List<CreateTableParser.ColumnInfo> columnInfosInSeq =
                columnInTemplate.values().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        CreateTableParser.ColumnInfo::getStartIndex))
                        .collect(Collectors.toList());
        for (CreateTableParser.ColumnInfo columnInfo : columnInfosInSeq) {
            String col = columnInfo.getName();
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol = columnToDorisType(column, typeConverter, caseSensitive);
                    String prefix = template.substring(0, columnInfo.getStartIndex() + offset);
                    String suffix = template.substring(offset + columnInfo.getEndIndex());
                    if (prefix.endsWith("`")) {
                        prefix = prefix.substring(0, prefix.length() - 1);
                        offset--;
                    }
                    if (suffix.startsWith("`")) {
                        suffix = suffix.substring(1);
                        offset--;
                    }
                    template = prefix + newCol + suffix;
                    offset += newCol.length() - columnInfo.getName().length();
                } else {
                    throw new IllegalArgumentException("Can't find column " + col + " in table.");
                }
            }
        }
        return template;
    }

    private static String columnToDorisType(
            Column column, TypeConverter<BasicTypeDefine> typeConverter, boolean caseSensitive) {
        if (column == null) {
            throw new IllegalArgumentException("The column is required.");
        }
        String columnName = caseSensitive ? column.getName() : column.getName().toLowerCase();
        return String.format(
                "`%s` %s %s %s",
                columnName,
                typeConverter.reconvert(column).getColumnType(),
                column.isNullable() ? "NULL" : "NOT NULL",
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '" + column.getComment() + "'");
    }
}

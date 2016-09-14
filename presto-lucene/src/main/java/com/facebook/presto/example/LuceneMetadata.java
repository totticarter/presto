/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.example.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;

//added by cubeli
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class LuceneMetadata
        implements ConnectorMetadata
{
    private final String connectorId;

//    private final LuceneClient luceneClient;
    
    private final Map<String, Map<String, LuceneTable>> schemas;

//    @Inject
    public LuceneMetadata(LuceneConnectorId connectorId, LuceneClient exampleClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
//        this.luceneClient = requireNonNull(exampleClient, "client is null");
        this.schemas = getSchemasFromHdfs();
    }

    private Map<String, Map<String, LuceneTable>> getSchemasFromHdfs() {
		
    	List<LuceneColumn> columns = new ArrayList<LuceneColumn>();
    	LuceneColumn lc1 = new LuceneColumn("custkey", BIGINT); columns.add(lc1);
    	LuceneColumn lc2 = new LuceneColumn("orderkey", BIGINT); columns.add(lc2);
    	LuceneColumn lc3 = new LuceneColumn("orderpriority", VARCHAR); columns.add(lc3);
    	LuceneColumn lc4 = new LuceneColumn("totalprice", DOUBLE); columns.add(lc4);
    	
    	LuceneTable lt = new LuceneTable("orders", columns);
    	
    	Map<String, LuceneTable> t1 = new HashMap<>();
    	t1.put("orders", lt);
    	
    	Map<String, Map<String, LuceneTable>> s1 = new HashMap<>();
    	s1.put("sf1", t1);
    	
		return s1;
	}

	@Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
//        return ImmutableList.copyOf(luceneClient.getSchemaNames());
    	return new ArrayList<>(schemas.keySet());
    }

    @Override
    public LuceneTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

//        LuceneTable table = luceneClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        LuceneTable table = getTableHandle(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new LuceneTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    private LuceneTable getTableHandle(String schemaName, String tableName) {
		
    	return schemas.get(schemaName).get(tableName);
	}

	@Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        LuceneTableHandle tableHandle = checkType(table, LuceneTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new LuceneTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LuceneTableHandle exampleTableHandle = checkType(table, LuceneTableHandle.class, "table");
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
//            schemaNames = luceneClient.getSchemaNames();
        	schemaNames = schemas.keySet();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
//            for (String tableName : luceneClient.getTableNames(schemaName)) {
        	for(String tableName: schemas.get(schemaName).keySet()){
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LuceneTableHandle exampleTableHandle = checkType(tableHandle, LuceneTableHandle.class, "tableHandle");
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

//        LuceneTable table = luceneClient.getTable(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());
        LuceneTable table = schemas.get(exampleTableHandle.getSchemaName()).get(exampleTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new LuceneColumnHandle(connectorId, column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

//        LuceneTable table = luceneClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        LuceneTable table = schemas.get(tableName.getSchemaName()).get(tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, LuceneTableHandle.class, "tableHandle");
        return checkType(columnHandle, LuceneColumnHandle.class, "columnHandle").getColumnMetadata();
    }
}

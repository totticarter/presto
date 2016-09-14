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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.example.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LuceneSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final LuceneClient exampleClient;
    
    //added by cubeli
    private final NodeManager nodeManager;
    private int splitsPerNode;

    //annotate by cubeli, for lucene do not need a client to get the schema info
//    @Inject
//    public LuceneSplitManager(LuceneConnectorId connectorId, LuceneClient exampleClient)
//    {
//        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
//        this.exampleClient = requireNonNull(exampleClient, "client is null");
//    }
    
//    @Inject
    public LuceneSplitManager(LuceneConnectorId connectorId, NodeManager nodeManager, int splitsPerNode)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = nodeManager;
        this.splitsPerNode = splitsPerNode;
        this.exampleClient = null;
    }
    


    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        LuceneTableLayoutHandle layoutHandle = checkType(layout, LuceneTableLayoutHandle.class, "layout");
        LuceneTableHandle tableHandle = layoutHandle.getTable();
        LuceneTable table = exampleClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

//        List<ConnectorSplit> splits = new ArrayList<>();
//        for (URI uri : table.getSources()) {
//            splits.add(new LuceneSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri));
//        }
//        Collections.shuffle(splits);
        
        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new LuceneSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), ImmutableList.of(node.getHostAndPort())));
            }
        }
        

        return new FixedSplitSource(splits.build());
    }
}

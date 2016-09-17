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
import com.facebook.presto.sql.parser.SqlBaseParser.LogicalBinaryContext;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import java.util.Set;

import static com.facebook.presto.example.Types.checkType;
import static java.util.Objects.requireNonNull;

import java.util.Map;

public class LuceneSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final LuceneClient exampleClient;
    
    //added by cubeli
    private final NodeManager nodeManager;
    private int splitsPerNode;
    private Expression pridcate;
    
//    @Inject
    public LuceneSplitManager(LuceneConnectorId connectorId, NodeManager nodeManager, int splitsPerNode)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = nodeManager;
        this.splitsPerNode = splitsPerNode;
        this.exampleClient = null;
    }
    


    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, Expression predicate)
    {
        LuceneTableLayoutHandle layoutHandle = checkType(layout, LuceneTableLayoutHandle.class, "layout");
        LuceneTableHandle tableHandle = layoutHandle.getTable();
        
//        String theDates
        getPartitions(predicate);
        
        
        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new LuceneSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), ImmutableList.of(node.getHostAndPort())));
            }
        }
        

        return new FixedSplitSource(splits.build());
    }
    
    //			IP			Table		Group,	Partition
    private Map<String, Map<String, Map<String, String>>> getHermesPartitionInfo(){
    	
    	
    	return null;
    }
    
    private void  getPartitions(Expression predicate){
    	
    	if(predicate instanceof LogicalBinaryExpression){
    		
    		LogicalBinaryExpression originalExp = (LogicalBinaryExpression)predicate;
    		Expression left1 = originalExp.getLeft();
    		if(left1 instanceof ComparisonExpression){
    			
    			ComparisonExpression ce = (ComparisonExpression)left1;
    			SymbolReference sr = (SymbolReference)ce.getLeft();
    			if(sr.getName().equals("custkey")){
    				
    				String name = sr.getName();
    				System.out.println(name);
    			}

    		}
    	}
    }
}

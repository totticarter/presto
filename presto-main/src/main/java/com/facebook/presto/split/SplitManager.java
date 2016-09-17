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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
//import com.facebook.presto.tpch.TpchTableLayoutHandle;
//import com.facebook.presto.example.LuceneSplitManager;
import com.facebook.presto.sql.tree.Expression;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class SplitManager
{
    private final ConcurrentMap<String, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

    public void addConnectorSplitManager(String connectorId, ConnectorSplitManager connectorSplitManager)
    {
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public SplitSource getSplits(Session session, TableLayoutHandle layout, Expression predicate)
    {
    	//added by cubeli start 使用这种方法会导致在构建项目时maven的jar包相互依赖的问题，因为底层依赖上层，现在上层又要依赖底层
//    	if(layout.getConnectorHandle() instanceof TpchTableLayoutHandle){
//    		
//    		TpchTableLayoutHandle tpchTableLayoutHandle = (TpchTableLayoutHandle)layout.getConnectorHandle();
//    		tpchTableLayoutHandle.setIndexPaths(null);
//    	}
    	
//      if(splitManager instanceof LuceneSplitManager){
//    	
//    	LuceneSplitManager splitManager = (LuceneSplitManager)splitManager;
//    	splitManager.setPridcate(predicate);
//    }
    	
    	
    	
        String connectorId = layout.getConnectorId();
        ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);  
        ConnectorSplitSource source = splitManager.getSplits(layout.getTransactionHandle(), connectorSession, layout.getConnectorHandle(), predicate); 

        return new ConnectorAwareSplitSource(connectorId, layout.getTransactionHandle(), source);
    }

    private ConnectorSplitManager getConnectorSplitManager(String connectorId)
    {
        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);
        return result;
    }
}

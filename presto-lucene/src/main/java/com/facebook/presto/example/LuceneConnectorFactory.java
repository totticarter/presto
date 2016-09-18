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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LuceneConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;
    
    //added by cubeli
    private final NodeManager nodeManager;

    public LuceneConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig, NodeManager nodeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
        
        //added by cubeli
        this.nodeManager = nodeManager;
    }

    @Override
    public String getName()
    {
        return "lucene";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LuceneHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig, ConnectorContext c)
    {
//        requireNonNull(requiredConfig, "requiredConfig is null");
//        try {
//            // A plugin is not required to use Guice; it is just very convenient
//            Bootstrap app = new Bootstrap(
//                    new JsonModule(),
//                    new LuceneModule(connectorId, typeManager));
//
//        Injector injector = app
//                    .strictConfig()
//                    .doNotInitializeLogging()
//                    .setRequiredConfigurationProperties(requiredConfig)
//                    .setOptionalConfigurationProperties(optionalConfig)
//                    .initialize();
//
//            return injector.getInstance(LuceneConnector.class);
//        }
//        catch (Exception e) {
//            throw Throwables.propagate(e);
//        }
    	
    	//=========================================================
//        int splitsPerNode = getSplitsPerNode(properties);
    	int splitsPerNode = 1;

        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return LuceneTransactionHandle.INSTANCE;
//            	return null;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new LuceneMetadata(new LuceneConnectorId(connectorId), null);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new LuceneSplitManager(new LuceneConnectorId(connectorId), nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new LuceneRecordSetProvider(new LuceneConnectorId(connectorId));
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                return new LuceneNodePartitioningProvider(connectorId, nodeManager, splitsPerNode);
//            	return null;
            }
        };
    	
    	
    }
}

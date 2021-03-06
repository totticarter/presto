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
package com.facebook.presto.transaction;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.sql.tree.Expression;

import static java.util.Objects.requireNonNull;

import org.apache.commons.math3.analysis.function.Exp;

public class LegacyConnectorSplitManager
        implements ConnectorSplitManager
{
    private final com.facebook.presto.spi.ConnectorSplitManager splitManager;

    public LegacyConnectorSplitManager(com.facebook.presto.spi.ConnectorSplitManager splitManager)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
    }

    @Override
    //aaaaa
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, Expression e)
    {
        return splitManager.getSplits(session, layout);
    }
}

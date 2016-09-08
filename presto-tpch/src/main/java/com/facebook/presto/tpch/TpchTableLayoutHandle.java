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
package com.facebook.presto.tpch;

import java.util.List;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
//import com.facebook.presto.sql.tree.Expression;

public class TpchTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TpchTableHandle table;
    
    //added by cubeli
    //正常情况下Expression不应该下沉到插件内部，所以这里暂时无法把expression传递下来。
//    private Expression e;
    private List<String> indexPaths;
    public void setIndexPaths(List<String> indexPaths){
    	
    	this.indexPaths = indexPaths; 
    }

    @JsonCreator
    public TpchTableLayoutHandle(@JsonProperty("table") TpchTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public TpchTableHandle getTable()
    {
        return table;
    }

    public String getConnectorId()
    {
        return table.getConnectorId();
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}

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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LuceneSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
//    private final URI uri;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;
    private final int partNumber;

    @JsonCreator
    public LuceneSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
//            @JsonProperty("uri") URI uri) //annotated by cubeli for lucene do not need a uri to get data
            @JsonProperty("addresses") List<HostAddress> addresses //added by cubeli
            )
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
//        this.uri = requireNonNull(uri, "uri is null");

//        if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
        remotelyAccessible = true;
//        addresses = ImmutableList.of(HostAddress.fromUri(uri));
        this.addresses = addresses;
        this.partNumber = 1;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    //annotated by cubeli 
    @JsonProperty
    public URI getUri()
    {
//        return uri;
    	return null;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }


    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

	public int getPartNumber() {
		// TODO Auto-generated method stub
		return partNumber;
	}
}

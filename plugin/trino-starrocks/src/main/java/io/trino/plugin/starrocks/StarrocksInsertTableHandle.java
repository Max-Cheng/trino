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
package io.trino.plugin.starrocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;

public class StarrocksInsertTableHandle
        implements ConnectorInsertTableHandle
{
    public final SchemaTableName schemaTableName;
    public final List<StarrocksColumnHandle> columns;
    public final String uuid;
    public final String host;

    @JsonCreator
    public StarrocksInsertTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columns") List<StarrocksColumnHandle> columns,
            @JsonProperty("uuid") String uuid,
            @JsonProperty("host") String host)
    {
        this.schemaTableName = schemaTableName;
        this.columns = columns;
        this.uuid = uuid;
        this.host = host;
    }

    @JsonProperty
    public String getUuid()
    {
        return this.uuid;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return this.schemaTableName;
    }

    @JsonProperty
    public List<StarrocksColumnHandle> getColumns()
    {
        return this.columns;
    }

    @JsonProperty
    public String getHost()
    {
        return this.host;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StarrocksInsertTableHandle that = (StarrocksInsertTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) && Objects.equals(columns, that.columns) && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columns, uuid);
    }
}

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import io.trino.type.JsonType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StarrocksValueEncoder
{
    private static final ObjectMapper mapper = new ObjectMapper();

    private StarrocksValueEncoder()
    {}

    static Object toJavaObject(ConnectorSession session, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof ArrayType arrayType) {
            Block cell = arrayType.getObject(block, position);
            List<Object> list = new ArrayList<>(cell.getPositionCount());
            for (int i = 0; i < cell.getPositionCount(); i++) {
                Object element = toJavaObject(session, arrayType.getElementType(), cell, i);
                list.add(element);
            }
            return list;
        }
        if (type instanceof RowType rowType) {
            SqlRow sqlRow = rowType.getObject(block, position);
            return toMap(session, rowType, sqlRow);
        }
        if (type instanceof MapType mapType) {
            SqlMap sqlMap = mapType.getObject(block, position);
            int rawOffset = sqlMap.getRawOffset();
            Block rawKeyBlock = sqlMap.getRawKeyBlock();
            Block rawValueBlock = sqlMap.getRawValueBlock();
            int size = sqlMap.getSize();
            Map<Object, Object> map = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                Object key = toJavaObject(session, mapType.getKeyType(), rawKeyBlock, rawOffset + i);
                Object value = toJavaObject(session, mapType.getValueType(), rawValueBlock, rawOffset + i);
                map.put(key, value);
            }
            return map;
        }
        if (type instanceof JsonType jsonType) {
            String jsonString = jsonType.getObjectValue(block, position).toString();
            try {
                return mapper.readValue(jsonString, new TypeReference<Map<Object, Object>>() {});
            }
            catch (JsonProcessingException e) {
                return jsonType.getObjectValue(block, position);
            }
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.getObjectValue(block, position) instanceof SqlDecimal sqlDecimal) {
                if (decimalType.getPrecision() == 38) {
                    return Int128.valueOf(sqlDecimal.getUnscaledValue()).toLongExact();
                }
                else {
                    return sqlDecimal.toString();
                }
            }
        }
        if (type instanceof DateType dateType) {
            return dateType.getObjectValue(block, position).toString();
        }
        return type.getObjectValue(block, position).toString();
    }

    static Map<String, Object> toMap(ConnectorSession session, RowType rowType, SqlRow sqlRow)
    {
        List<RowType.Field> fields = rowType.getFields();
        Map<String, Object> map = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            String key = field.getName().orElse("field" + i);
            sqlRow.getRawFieldBlock(i);
            Object value = toJavaObject(session, field.getType(), sqlRow.getRawFieldBlock(i), sqlRow.getRawIndex());
            map.put(key, value);
        }
        return map;
    }
}

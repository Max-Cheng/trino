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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class StarrocksSessionProperties
{
    private static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    private static final String TUPLE_DOMAIN_LIMIT = "tuple_domain_limit";
    private static final String DEFAULT_STREAM_LOAD_COMPRESSION = "default_stream_load_compression";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StarrocksSessionProperties(StarrocksConfig starrocksConfig)
    {
        sessionProperties = ImmutableList.of(
                durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters",
                        starrocksConfig.getDynamicFilteringWaitTimeout(),
                        false),
                integerProperty(
                        TUPLE_DOMAIN_LIMIT,
                        "Maximum number of tuple domains to include in a single dynamic filter",
                        starrocksConfig.getTupleDomainLimit(),
                        false),
                stringProperty(
                        DEFAULT_STREAM_LOAD_COMPRESSION,
                        "Default Stream Load compression, e.g. none, gzip, bzip2, lz4_frame, zstd.",
                        starrocksConfig.getDefaultStreamLoadCompression(),
                        false));
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static int getTupleDomainLimit(ConnectorSession session)
    {
        return session.getProperty(TUPLE_DOMAIN_LIMIT, Integer.class);
    }

    public static String getDefaultStreamLoadCompression(ConnectorSession session)
    {
        return session.getProperty(DEFAULT_STREAM_LOAD_COMPRESSION, String.class);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}

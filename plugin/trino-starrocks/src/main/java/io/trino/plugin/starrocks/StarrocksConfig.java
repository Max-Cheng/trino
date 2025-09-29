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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class StarrocksConfig
{
    private List<String> scanURL;
    private String jdbcURL;
    private List<String> loadURL = ImmutableList.of();
    private String username;
    private String password;
    private final int scanMaxRetry = 3;
    private Duration dynamicFilteringWaitTimeout = Duration.valueOf("10s");
    private int tupleDomainLimit = 10000;
    private boolean enableLDAP;
    private boolean enableMaterializedView;

    private String defaultStreamLoadCompression = "none";

    @NotNull
    public String getScanURL()
    {
        return scanURL.get(new Random().nextInt(this.scanURL.size()));
    }

    @Config("starrocks.scan-url")
    @ConfigDescription("Scan URL for the Starrocks BE")
    public StarrocksConfig setScanURL(String scanURL)
    {
        this.scanURL = Arrays.stream(scanURL.split(","))
                .collect(toImmutableList());
        return this;
    }

    @NotNull
    public List<String> getLoadURL()
    {
        return this.loadURL;
    }

    @Config("starrocks.load-url")
    @ConfigDescription("The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (,)")
    public StarrocksConfig setLoadURL(String loadURL)
    {
        this.loadURL = Arrays.stream(loadURL.split(","))
                .collect(toImmutableList());
        return this;
    }

    public String getJdbcURL()
    {
        return jdbcURL;
    }

    @Config("starrocks.jdbc-url")
    @ConfigDescription("JDBC URL for the Starrocks FE")
    public StarrocksConfig setJdbcURL(String jdbcURL)
    {
        this.jdbcURL = jdbcURL;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("starrocks.username")
    @ConfigDescription("Username for the Starrocks user")
    public StarrocksConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @MinDuration("0ms")
    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("starrocks.dynamic-filtering-wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public StarrocksConfig setDynamicFilteringWaitTimeout(String dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = Duration.valueOf(dynamicFilteringWaitTimeout);
        return this;
    }

    @Config("starrocks.tuple-domain-limit")
    @ConfigDescription("Maximum number of tuple domains to include in a single dynamic filter")
    public StarrocksConfig setTupleDomainLimit(int tupleDomainLimit)
    {
        this.tupleDomainLimit = tupleDomainLimit;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("starrocks.password")
    @ConfigDescription("Password for the Starrocks user")
    @ConfigSecuritySensitive
    public StarrocksConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public int getScanMaxRetries()
    {
        return scanMaxRetry;
    }

    public int getTupleDomainLimit()
    {
        return tupleDomainLimit;
    }

    public boolean isEnableLDAP()
    {
        return enableLDAP;
    }

    @Config("starrocks.enable-ldap")
    public StarrocksConfig setEnableLDAP(boolean enableLDAP)
    {
        this.enableLDAP = enableLDAP;
        return this;
    }

    public String getDefaultStreamLoadCompression()
    {
        return defaultStreamLoadCompression;
    }

    @Config("default-stream-load-compression")
    @ConfigDescription("Defualt Stream Load compression, e.g. none, gzip, bzip2, lz4_frame, zstd.")
    public StarrocksConfig setDefaultStreamLoadCompression(String defaultStreamLoadCompression)
    {
        this.defaultStreamLoadCompression = defaultStreamLoadCompression;
        return this;
    }

    public boolean getDefaultMaterializedViewEnabled()
    {
        return enableMaterializedView;
    }

    @Config("starrocks.enable-materialized-view")
    @ConfigDescription("Some materialized view can be query")
    public StarrocksConfig setDefaultMaterializedViewEnabled(boolean enableMaterializedView)
    {
        this.enableMaterializedView = enableMaterializedView;
        return this;
    }
}

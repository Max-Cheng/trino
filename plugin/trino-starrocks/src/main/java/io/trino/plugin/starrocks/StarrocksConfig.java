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
    private Optional<String> password;
    private final int scanMaxRetry = 3;
    private Duration dynamicFilteringWaitTimeout = Duration.valueOf("10s");
    private int tupleDomainLimit = 1000;
    private boolean enableLDAP;

    private String defaultStreamLoadCompression = "none";

    @NotNull
    public String getScanURL()
    {
        return scanURL.get(new Random().nextInt(this.scanURL.size()));
    }

    @Config("scan-url")
    @ConfigDescription("Scan URL for the Starrocks BE")
    public StarrocksConfig setScanURL(String scanURL)
    {
        this.scanURL = Arrays.stream(scanURL.split(","))
                .collect(toImmutableList());
        return this;
    }

    @Config("load-url")
    @ConfigDescription("The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (,)")
    public StarrocksConfig setLoadURL(String loadURL)
    {
        this.loadURL = Arrays.stream(loadURL.split(","))
                .collect(toImmutableList());
        return this;
    }

    @NotNull
    public List<String> getLoadURL()
    {
        return this.loadURL;
    }

    public String getJdbcURL()
    {
        return jdbcURL;
    }

    @Config("jdbc-url")
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

    @Config("username")
    @ConfigDescription("Username for the Starrocks user")
    public StarrocksConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("dynamic-filtering-wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public Duration setDynamicFilteringWaitTimeout(String dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = Duration.valueOf(dynamicFilteringWaitTimeout);
        return this.dynamicFilteringWaitTimeout;
    }

    @Config("tuple-domain-limit")
    @ConfigDescription("Maximum number of tuple domains to include in a single dynamic filter")
    public int setTupleDomainLimit(int tupleDomainLimit)
    {
        this.tupleDomainLimit = tupleDomainLimit;
        return this.tupleDomainLimit;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @Config("password")
    @ConfigDescription("Password for the Starrocks user")
    public StarrocksConfig setPassword(String password)
    {
        this.password = Optional.of(password);
        return this;
    }

    public int getScanMaxRetries()
    {
        return scanMaxRetry;
    }

    @MinDuration("0ms")
    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    public int getTupleDomainLimit()
    {
        return tupleDomainLimit;
    }

    public boolean isEnableLDAP()
    {
        return enableLDAP;
    }

    @Config("enable-ldap")
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
    public String setDefaultStreamLoadCompression(String defaultStreamLoadCompression)
    {
        this.defaultStreamLoadCompression = defaultStreamLoadCompression;
        return this.defaultStreamLoadCompression;
    }
}

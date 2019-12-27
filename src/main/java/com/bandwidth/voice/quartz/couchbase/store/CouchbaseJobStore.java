package com.bandwidth.voice.quartz.couchbase.store;

import com.couchbase.client.encryption.CryptoManager;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.spi.JobStore;

@Slf4j
@NoArgsConstructor
public abstract class CouchbaseJobStore implements JobStore {

    @Setter
    protected String instanceName;
    @Setter
    protected String instanceId;
    @Setter
    protected int threadPoolSize;

    private DefaultCouchbaseEnvironment.Builder environment = DefaultCouchbaseEnvironment.builder();
    @Setter
    protected String clusterNodes;
    @Setter
    protected String clusterUsername;
    @Setter
    protected String clusterPassword;
    @Setter
    protected String bucketName;
    @Setter
    protected String bucketPassword;

    public void setManagementTimeout(long managementTimeout) {
        environment.managementTimeout(managementTimeout);
    }

    public void setQueryTimeout(long queryTimeout) {
        environment.queryTimeout(queryTimeout);
    }

    public void setViewTimeout(long viewTimeout) {
        environment.viewTimeout(viewTimeout);
    }

    public void setKvTimeout(long kvTimeout) {
        environment.kvTimeout(kvTimeout);
    }

    public void setSearchTimeout(long searchTimeout) {
        environment.searchTimeout(searchTimeout);
    }

    public void setAnalyticsTimeout(long analyticsTimeout) {
        environment.analyticsTimeout(analyticsTimeout);
    }

    public void setConnectTimeout(long connectTimeout) {
        environment.connectTimeout(connectTimeout);
    }

    public void setDnsSrvEnabled(boolean dnsSrvEnabled) {
        environment.dnsSrvEnabled(dnsSrvEnabled);
    }

    public void setCryptoManager(CryptoManager cryptoManager) {
        environment.cryptoManager(cryptoManager);
    }

    public void setPropagateParentSpan(boolean propagateParentSpan) {
        environment.propagateParentSpan(propagateParentSpan);
    }

    public DefaultCouchbaseEnvironment getEnvironment() {
        return environment.build();
    }
}

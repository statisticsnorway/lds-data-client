package no.ssb.lds.data.service;

import no.ssb.gsim.client.GsimClient;
import no.ssb.lds.data.GoogleCloudStorageBackend;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.common.parquet.ParquetProvider;

public class Configuration {

    private GsimClient.Configuration gsim;
    private DataClient.Configuration data;
    private GoogleCloudStorageBackend.Configuration googleCloud;
    private ParquetProvider.Configuration parquet;
    private CachedBackend.Configuration cache;
    private String host;
    private Integer port;

    public Configuration() {
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public DataClient.Configuration getData() {
        return data;
    }

    public void setData(DataClient.Configuration data) {
        this.data = data;
    }

    public ParquetProvider.Configuration getParquet() {
        return parquet;
    }

    public void setParquet(ParquetProvider.Configuration parquet) {
        this.parquet = parquet;
    }

    public CachedBackend.Configuration getCache() {
        return cache;
    }

    public void setCache(CachedBackend.Configuration cache) {
        this.cache = cache;
    }

    public GsimClient.Configuration getGsim() {
        return gsim;
    }

    public void setGsim(GsimClient.Configuration gsim) {
        this.gsim = gsim;
    }

    public GoogleCloudStorageBackend.Configuration getGoogleCloud() {
        return googleCloud;
    }

    public void setGoogleCloud(GoogleCloudStorageBackend.Configuration googleCloud) {
        this.googleCloud = googleCloud;
    }
}

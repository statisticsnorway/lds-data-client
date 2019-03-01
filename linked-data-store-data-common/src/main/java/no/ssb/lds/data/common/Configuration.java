package no.ssb.lds.data.common;

import java.net.URL;

public class Configuration {

    private String host;
    private Integer port;
    private URL ldsServer;
    private String graphqlPath;
    private String dataPrefix;
    private GoogleCloud googleCloud;
    private Parquet parquet;

    public Configuration() {
    }

    public String getGraphqlPath() {
        return graphqlPath;
    }

    public void setGraphqlPath(String graphqlPath) {
        this.graphqlPath = graphqlPath;
    }

    public GoogleCloud getGoogleCloud() {
        return googleCloud;
    }

    public void setGoogleCloud(GoogleCloud googleCloud) {
        this.googleCloud = googleCloud;
    }

    public Parquet getParquet() {
        return parquet;
    }

    public void setParquet(Parquet parquet) {
        this.parquet = parquet;
    }

    public String getDataPrefix() {
        return dataPrefix;
    }

    public void setDataPrefix(String dataPrefix) {
        this.dataPrefix = dataPrefix;
    }

    public URL getLdsServer() {
        return ldsServer;
    }

    public void setLdsServer(URL ldsServer) {
        this.ldsServer = ldsServer;
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

    public static class GoogleCloud {

        private Integer readChunkSize;
        private Integer writeChunkSize;

        public GoogleCloud() {
        }

        public Integer getReadChunkSize() {
            return readChunkSize;
        }

        public void setReadChunkSize(Integer readChunkSize) {
            this.readChunkSize = readChunkSize;
        }

        public Integer getWriteChunkSize() {
            return writeChunkSize;
        }

        public void setWriteChunkSize(Integer writeChunkSize) {
            this.writeChunkSize = writeChunkSize;
        }
    }

    public static class Parquet {

        private Integer rowGroupSize;
        private Integer pageSize;

        public Parquet() {
        }

        public Integer getRowGroupSize() {
            return rowGroupSize;
        }

        public void setRowGroupSize(Integer rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public void setPageSize(Integer pageSize) {
            this.pageSize = pageSize;
        }

    }
}

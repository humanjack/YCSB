package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticsearchTransportClient extends ElasticSearchClient{
    @Override
    public void init() throws DBException {
        // initialize OrientDB driver
        Properties props = getProperties();
        this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);
        String clusterName = props.getProperty("cluster.name", DEFAULT_CLUSTER_NAME);
        String[] hostList = props.getProperty("es.hosts", "localhost").split(",");
        int port = Integer.parseInt(props.getProperty("es.port", "9300"));
        Boolean newdb = Boolean.parseBoolean(props.getProperty("elasticsearch.newdb", "false"));

        Settings settings =
                ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", "true").build();

        client = new TransportClient(settings);

        for (String host : hostList) {
            ((TransportClient)client).addTransportAddress(new InetSocketTransportAddress(host, port));
        }

        if (newdb) {
            client.admin().indices().prepareDelete(indexKey).execute().actionGet();
            client.admin().indices().prepareCreate(indexKey).execute().actionGet();
        } else {
            boolean exists = client.admin().indices().exists(Requests.indicesExistsRequest(indexKey)).actionGet().isExists();
            if (!exists) {
                client.admin().indices().prepareCreate(indexKey).execute().actionGet();
            }
        }
    }

    @Override
    public void cleanup() throws DBException {
        client.close();
    }
}

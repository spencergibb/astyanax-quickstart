package com.github.boneill42;

import java.nio.ByteBuffer;
import java.util.Map;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.CqlResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxDao {
    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxDao.class);
    private Keyspace keyspace;
    private AstyanaxContext<Keyspace> astyanaxContext;

    public AstyanaxDao(String host, String keyspace) {
        try {
            this.astyanaxContext = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(keyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                            .setDiscoveryType(NodeDiscoveryType.NONE)
                            .setCqlVersion("3.0.0")
                            .setTargetCassandraVersion("1.2")
                    )
                    .withConnectionPoolConfiguration(
                            new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1)
                                    .setSeeds(host)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            this.astyanaxContext.start();
            this.keyspace = this.astyanaxContext.getEntity();
            // test the connection
            this.keyspace.describeKeyspace();
        } catch (Throwable e) {
            LOG.warn("Preparation failed.", e);
            throw new RuntimeException("Failed to prepare CassandraBolt", e);
        }
    }

    public void cleanup() {
        this.astyanaxContext.shutdown();
    }

    /**
     * Writes columns.
     */
    public void write(String columnFamilyName, String rowKey, Map<String, String> columns) throws ConnectionException {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
        mutation.execute();
    }
    
    /**
     * Writes compound/composite columns.
     */
    public void writeBlog(String columnFamilyName, String rowKey, FishBlogColumn blog, byte[] value) throws ConnectionException {
        AnnotatedCompositeSerializer<FishBlogColumn> entitySerializer = new AnnotatedCompositeSerializer<FishBlogColumn>(FishBlogColumn.class);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, FishBlogColumn> columnFamily = new ColumnFamily<String, FishBlogColumn>(columnFamilyName,
                StringSerializer.get(), entitySerializer);
        mutation.withRow(columnFamily, rowKey).putColumn(blog, value, null);
        mutation.execute();
    }

    public void writeBlog(String columnFamilyName, String rowKey, FishBlogColumn... columns) throws ConnectionException {
        AnnotatedCompositeSerializer<FishBlogColumn.Key> entitySerializer = new AnnotatedCompositeSerializer<FishBlogColumn.Key>(FishBlogColumn.Key.class);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, FishBlogColumn.Key> columnFamily = new ColumnFamily<String, FishBlogColumn.Key>(columnFamilyName,
                StringSerializer.get(), entitySerializer);

        ColumnListMutation<FishBlogColumn.Key> row = mutation.withRow(columnFamily, rowKey);
        for (FishBlogColumn column: columns) {
            row.putColumn(column.key, column.value, null);
        }
        mutation.execute();
    }

    /**
     * Fetches an entire row.
     */
    public ColumnList<String> read(String columnFamilyName, String rowKey) throws ConnectionException {
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        OperationResult<ColumnList<String>> result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();
        return result.getResult();
    }

    /**
     * Fetches an entire row using composite keys
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public ColumnList<FishBlogColumn.Key> readBlogs(String columnFamilyName, String rowKey) throws ConnectionException {
        AnnotatedCompositeSerializer<FishBlogColumn.Key> entitySerializer = new AnnotatedCompositeSerializer<FishBlogColumn.Key>(FishBlogColumn.Key.class);
        ColumnFamily<String, FishBlogColumn.Key> columnFamily = new ColumnFamily<String, FishBlogColumn.Key>(columnFamilyName,
                StringSerializer.get(), entitySerializer);
        OperationResult<ColumnList<FishBlogColumn.Key>> result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();
        return result.getResult();
    }

    public CqlResult<String, String> readWithCql(String columnFamilyName, String userid) throws ConnectionException {
        final String SELECT_STATEMENT = "SELECT * FROM fishblogs WHERE userid=?;";

        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(
                columnFamilyName,
                StringSerializer.get(),
                StringSerializer.get());

        OperationResult<CqlResult<String, String>> result = keyspace.prepareQuery(columnFamily)
                .withCql(SELECT_STATEMENT)
                .asPreparedStatement()
                .withStringValue(userid)
                .execute();

        return result.getResult();
    }

    public OperationResult<CqlResult<String, String>> writeWithCql(String columnFamilyName) throws ConnectionException {
        final String INSERT_STATEMENT = "INSERT INTO fishblogs (userid, when, fishtype, blog, image) VALUES (?, ?, ?, ?, ?);";

        ColumnFamily<String, String> columnFamily = ColumnFamily.newColumnFamily(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());

        byte[] bytes = new byte[10];
        bytes[0] = 2;
        OperationResult<CqlResult<String, String>> result = keyspace.prepareQuery(columnFamily)
                .withCql(INSERT_STATEMENT)
                .asPreparedStatement()
                .withStringValue("bigcat")
                .withLongValue(System.currentTimeMillis())
                .withStringValue("TROUT")
                .withStringValue("this is more blog")
                .withValue(ByteBuffer.wrap(bytes))
                .execute();

        return result;
    }
}

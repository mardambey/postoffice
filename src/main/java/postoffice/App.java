package postoffice;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

public class App
{	
    public static void main( String[] args ) throws Exception
    {
    	String pool = "pool";
    	String keyspace = "hmb";
    	String colFamily = "users";

    	// init the connection pool
    	Cluster cluster = new Cluster("localhost", 9160);
    	Pelops.addPool(pool, cluster, keyspace);

    	String rowKey = "abc123";

    	// write out some data
    	Mutator mutator = Pelops.createMutator(pool);
    	mutator.writeColumns(
    	        colFamily, rowKey,
    	        mutator.newColumnList(
    	                mutator.newColumn("name", "Dan"),
    	                mutator.newColumn("age", Bytes.fromInt(33))
    	        )
    	);
    	mutator.execute(ConsistencyLevel.ONE);

    	// read back the data we just wrote
    	Selector selector = Pelops.createSelector(pool);
    	List<Column> columns = selector.getColumnsFromRow(colFamily, rowKey, false, ConsistencyLevel.ONE);

    	System.out.println("Name: " + Selector.getColumnStringValue(columns, "name"));
    	System.out.println("Age: " + Selector.getColumnValue(columns, "age").toInt());

    	// shut down the pool
    	Pelops.shutdown();
    }
}

package uconn.cse.cassperf;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
//import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
//import me.prettyprint.hector.api.Cluster;
//import me.prettyprint.hector.api.HConsistencyLevel;
//import me.prettyprint.hector.api.Keyspace;
//import me.prettyprint.hector.api.factory.HFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CassPerfDatastaxBase {

	protected static Cluster cassPerfCluster;
	protected static Session cassPerfSession;
	// protected static Keyspace computationalKeyspace;
	protected static Properties properties;


	protected static void initializeDatastaxLib() {
		properties = new Properties();
		try {
			properties.load(new FileInputStream("gendata.properties"));
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		// To modify the default ConsistencyLevel of QUORUM, create a
		// me.prettyprint.hector.api.ConsistencyLevelPolicy and use the
		// overloaded form:
		// healthCareKeyspace = HFactory.createKeyspace("Tutorial",
		// hcSystemCluster, consistencyLevelPolicy);
		// see also
		// me.prettyprint.cassandra.model.ConfigurableConsistencyLevelPolicy[Test]
		// for details
		try {
		cassPerfCluster = Cluster
				.builder()
				.addContactPoint(
						properties
								.getProperty("cql.cluster.hosts", "127.0.0.1"))
				.withClusterName(
						properties.getProperty("cluster.name",
								"CassPerfCluster"))
				.withPort(
						Integer.parseInt(properties.getProperty("cql.port",
								"9042"))).build();
	     Metadata metadata = cassPerfCluster.getMetadata();
	       System.out.printf("Connected to cluster: %s\n", 
	             metadata.getClusterName());
	       for ( Host host : metadata.getAllHosts() ) {
	          System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
	             host.getDatacenter(), host.getAddress(), host.getRack());
	       }
		// cassPerfCluster = HFactory.getOrCreateCluster(
		// properties.getProperty("cluster.name", "CassPerfCluster"),
		// properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
		// hcSystemCluster = HFactory.getOrCreateCluster("HealthCareCluster",
		// "10.12.2.209:9160");
		// ConfigurableConsistencyLevel configConsistencyLevel = new
		// ConfigurableConsistencyLevel();
		// configConsistencyLevel
		// .setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		// configConsistencyLevel
		// .setDefaultWriteConsistencyLevel(HConsistencyLevel.ANY);

		// healthCareKeyspace =
		// HFactory.createKeyspace(properties.getProperty("healthcare.keyspace",
		// "HealthCare"), hcSystemCluster, ccl);
		cassPerfSession = cassPerfCluster.connect();
		// cassPerfKeyspace = HFactory.createKeyspace("CassExp",
		// cassPerfCluster,
		// configConsistencyLevel);
		} catch (Exception e) {
		  e.printStackTrace();
		}

	}

	public static Cluster getCluster() {
		return cassPerfCluster;
	}

	public static Session getDataKeyspace() {
		return cassPerfSession;
	}

	public static void close() {
		cassPerfCluster.close();
	}
}

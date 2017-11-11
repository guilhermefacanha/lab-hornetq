package org.gf.lab.hornetq.connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.jms.client.HornetQConnectionFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * Connection class to HornetQ
 * <br/>It will manage n number os host
 * <br/>To configure the hosts you must put System Properties like, the .N is the number of the host:
 		<br/>====================
 		<br/>System.setProperty(HornetQConnection.HOST_PROPERTY + ".0", "localhost");
		<br/>System.setProperty(HornetQConnection.PORT_PROPERTY + ".0", "5445");
		<br/>System.setProperty(HornetQConnection.USER_PROPERTY + ".0", "guest");
		<br/>System.setProperty(HornetQConnection.PASS_PROPERTY + ".0", "guest");
 		<br/>====================
 		<br/>System.setProperty(HornetQConnection.HOST_PROPERTY + ".1", "localhost");
		<br/>System.setProperty(HornetQConnection.PORT_PROPERTY + ".1", "5545");
		<br/>System.setProperty(HornetQConnection.USER_PROPERTY + ".1", "guest");
		<br/>System.setProperty(HornetQConnection.PASS_PROPERTY + ".1", "guest");
 		<br/>====================
 * */
@Slf4j
public class HornetQConnection {

	public static final String HOST_PROPERTY = "hornetq.host";
	public static final String PORT_PROPERTY = "hornetq.port";
	public static final String USER_PROPERTY = "hornetq.user";
	public static final String PASS_PROPERTY = "hornetq.pass";

	private static List<Connection> connections = null;
	
	private static int numberHosts = 0;

	/**
	 * Provide a session to a hornetQ server
	 * @param host - index of the host to get session
	 * */
	public static Session getSession(int host) throws JMSException {
		return getConnection(host).createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	private static Connection getConnection(int host) throws JMSException {
		if (connections==null)
			createConnections();

		if (host > numberHosts)
			throw new RuntimeException("O número do host não está configurado");


		return connections.get(host);
	}

	private static void createConnections() throws JMSException {
		discoverNumberOfHosts();
		connections = new ArrayList<Connection>(numberHosts);
		for (int i = 0; i <= numberHosts; i++) {
			createConnection(i);
		}
	}

	private static void createConnection(int idx) throws JMSException {
		Map<String, Object> connectionsParams = getConnectionsParams(idx);
		String user = connectionsParams.get("user").toString();
		String pass = connectionsParams.get("pass").toString();
		
		connectionsParams.remove("user");
		connectionsParams.remove("pass");
		
		TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionsParams);
		HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
		Connection connection = cf.createConnection(user,pass);
		connection.start();

		connections.add(connection);
	}

	private static void discoverNumberOfHosts() {
		log.debug("Starting the discovery of hosts");
		int idx = 1;
		while (System.getProperty(HOST_PROPERTY + "." + idx) != null) {
			numberHosts++;
			idx++;
		}
		log.debug("Number of hosts: {}", numberHosts);
	}

	private static Map<String, Object> getConnectionsParams(int idx) {
		Map<String, Object> connectionParams = new HashMap<String, Object>();
		if (idx == 0) {
			String host = System.getProperty(HOST_PROPERTY + "." + idx, "localhost");
			String port = System.getProperty(PORT_PROPERTY + "." + idx, "5445");

			String user = System.getProperty(USER_PROPERTY + "." + idx, "guest");
			String pass = System.getProperty(PASS_PROPERTY + "." + idx, "guest");

			connectionParams.put(TransportConstants.HOST_PROP_NAME, host);
			connectionParams.put(TransportConstants.PORT_PROP_NAME, port);
			connectionParams.put("user", user);
			connectionParams.put("pass", pass);
			log.debug("Configuring host 0 with host:{}; port:{}; user:{}; pass:*****", host, port, user);

			return connectionParams;
		} else {
			String host = System.getProperty(HOST_PROPERTY + "." + idx);
			String port = System.getProperty(PORT_PROPERTY + "." + idx);

			String user = System.getProperty(USER_PROPERTY + "." + idx);
			String pass = System.getProperty(PASS_PROPERTY + "." + idx);

			verifyParams(host, port, user, pass, idx);

			connectionParams.put(TransportConstants.HOST_PROP_NAME, host);
			connectionParams.put(TransportConstants.PORT_PROP_NAME, port);
			connectionParams.put("user", user);
			connectionParams.put("pass", pass);
			log.debug("Configuring host {} with host:{}; port:{}; user:{}; pass:*****", idx, host, port, user);

			return connectionParams;
		}

	}

	private static void verifyParams(String host, String port, String user, String pass, int idx) {
		if (host == null)
			throw new RuntimeException("Unable to load host server param for host index " + idx);
		if (port == null)
			throw new RuntimeException("Unable to load port param for host index " + idx);
		if (user == null)
			throw new RuntimeException("Unable to load user param for host index " + idx);
		if (pass == null)
			throw new RuntimeException("Unable to load password param for host index " + idx);

	}

}

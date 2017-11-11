package org.gf.lab.hornetq.service;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.gf.lab.hornetq.connection.HornetQConnection;
import org.hornetq.api.jms.HornetQJMSClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Service class to provide message producer and consumer
 * */
@Slf4j
public class HornetQMessageService {

	private String QUEUE_NAME;
	private int host = 0;
	Queue queue = null;

	/**
	 * Constructor method to connect to the default/first Host of index '0'
	 * @param queuename - name of hornetQ queue to connect
	 * */
	public HornetQMessageService(String queuename) {
		this.QUEUE_NAME = queuename;
		this.queue = HornetQJMSClient.createQueue(QUEUE_NAME);
	}

	/**
	 * Constructor method to connect to the specified Host
	 * @param queuename - name of hornetQ queue to connect
	 * @param host - index of the host
	 * */
	public HornetQMessageService(String queuename, int host) {
		this.QUEUE_NAME = queuename;
		this.queue = HornetQJMSClient.createQueue(QUEUE_NAME);
		this.host = host;
	}

	/**
	 * Send text message to HornetQ
	 * @param msg - message {@link String}
	 * */
	public void sendTextMessage(String msg) throws JMSException {
		Session session = HornetQConnection.getSession(host);
		try {
			MessageProducer producer = session.createProducer(queue);
			TextMessage message = session.createTextMessage(msg);
			log.debug("Enviando mensagem: {}", msg);
			producer.send(message);
		} finally {
			session.close();
		}
	}

	/**
	 * Send object message to HornetQ, the object must be Serialized
	 * @param msg T extends {@link Serializable}
	 * */
	public <T extends Serializable> void sendObjectMessage(T msg) throws JMSException {
		Session session = HornetQConnection.getSession(host);
		try {
			MessageProducer producer = session.createProducer(queue);
			ObjectMessage message = session.createObjectMessage();
			message.setObject(msg);
			log.debug("Enviando mensagem: {}", msg);
			producer.send(message);
		} finally {
			session.close();
		}
	}

	/**
	 * Get text message from HornetQ
	 * @return {@link String}
	 * */
	public String getTextMessage() throws JMSException {
		Session session = HornetQConnection.getSession(host);
		try {
			MessageConsumer messageConsumer = session.createConsumer(queue);
			TextMessage receive = (TextMessage) messageConsumer.receive(5000);
			if (receive != null)
				return receive.getText();
			else
				return null;
		} finally {
			session.close();
		}
	}

	/**
	 * Get text message from HornetQ
	 * @return {@link ObjectMessage}
	 * */
	public ObjectMessage getObjectMessage() throws JMSException {
		Session session = HornetQConnection.getSession(host);
		try {
			MessageConsumer messageConsumer = session.createConsumer(queue);
			ObjectMessage receive = (ObjectMessage) messageConsumer.receive(5000);
			if (receive != null)
				return receive;
			else
				return null;
		} finally {
			session.close();
		}
	}

}

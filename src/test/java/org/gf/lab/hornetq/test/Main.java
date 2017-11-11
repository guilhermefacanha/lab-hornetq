package org.gf.lab.hornetq.test;

import java.io.IOException;
import java.util.HashMap;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.gf.lab.hornetq.connection.HornetQConnection;
import org.gf.lab.hornetq.service.HornetQMessageService;
import org.hornetq.utils.Random;

import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("unused")
@Slf4j
public class Main {

	static HornetQMessageService hornetQMessageUtilServer1 = new HornetQMessageService("MensagemServer1Queue");
	static HornetQMessageService hornetQMessageUtilServer2 = new HornetQMessageService("MensagemServer2Queue", 1);

	public static void main(String[] args) throws IOException, JMSException {
		initConfig();

		// TO DEBUG MODE RUN CONFIGURATIONS ADD VM ARGS
		// -Dorg.slf4j.simpleLogger.defaultLogLevel=debug
		log.debug("===DEBUG MODE===");
		log.info("====INICIO DE APP LAB HORNET Q===");

		// inserirMensagensObject();
		// lerMensagensObjeto();

		inserirMensagens();
		lerMensagens();

		 inserirMensagens2();
		 lerMensagens2();

		log.info("====FIM DE APP LAB HORNET Q===");
	}

	private static void initConfig() {
		System.setProperty(HornetQConnection.PASS_PROPERTY + ".0", "1qaz@WSX");

		System.setProperty(HornetQConnection.HOST_PROPERTY + ".1", "localhost");
		System.setProperty(HornetQConnection.PORT_PROPERTY + ".1", "5545");
		System.setProperty(HornetQConnection.USER_PROPERTY + ".1", "guest");
		System.setProperty(HornetQConnection.PASS_PROPERTY + ".1", "1qaz@WSX");
	}

	private static void lerMensagens() throws JMSException {
		String msg = hornetQMessageUtilServer1.getTextMessage();
		while (msg != null) {
			log.info("Mensagem recebida do server 1: {}", msg);
			msg = hornetQMessageUtilServer1.getTextMessage();
		}
		log.info("Nenhuma mensagem na fila");
	}

	private static void lerMensagens2() throws JMSException {
		String msg = hornetQMessageUtilServer2.getTextMessage();
		while (msg != null) {
			log.info("Mensagem recebida do server 2: {}", msg);
			msg = hornetQMessageUtilServer2.getTextMessage();
		}
		log.info("Nenhuma mensagem na fila");
	}

	private static void lerMensagensObjeto() throws JMSException {
		ObjectMessage msg = hornetQMessageUtilServer1.getObjectMessage();
		while (msg != null) {
			@SuppressWarnings("unchecked")
			HashMap<Long, String> mapa = (HashMap<Long, String>) msg.getObject();
			log.info("Mensagem recebida do server 1: {}", msg);
			log.info("Objeto recebido do server 1: {}", mapa);
			msg = hornetQMessageUtilServer1.getObjectMessage();
		}
		log.info("Nenhuma mensagem na fila");
	}

	private static void inserirMensagensObject() throws IOException, JMSException {
		HashMap<Long, String> mapa = new HashMap<Long, String>();
		mapa.put(1l, "valor 1");
		mapa.put(2l, "valor 2");
		mapa.put(3l, "valor 3");
		hornetQMessageUtilServer1.sendObjectMessage(mapa);
		log.info("MENSAGEM OBJETO ENVIADA COM SUCESSO");
	}

	private static void inserirMensagens() throws IOException, JMSException {
		Integer n = new Random().getRandom().nextInt(10);
		log.debug("Generating {} messages", n);
		for (int i = 0; i < n; i++) {
			String msg = "GENERATEDE MESSAGE TO SERVER 1 - " + (i + 1);
			hornetQMessageUtilServer1.sendTextMessage(msg);
			log.info("Mensagem enviada para SERVER 1 {}", msg);
		}
	}

	private static void inserirMensagens2() throws IOException, JMSException {
		Integer n = new Random().getRandom().nextInt(10);
		log.debug("Generating {} messages", n);
		for (int i = 0; i < n; i++) {
			String msg = "GENERATEDE MESSAGE TO SERVER 2 - " + (i + 1);
			hornetQMessageUtilServer2.sendTextMessage(msg);
			log.info("Mensagem enviada para SERVER 2 {}", msg);
		}
	}
}

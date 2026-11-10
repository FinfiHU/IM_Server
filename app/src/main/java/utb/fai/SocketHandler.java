package utb.fai;

import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class SocketHandler {
	private static final Pattern WHITESPACE = Pattern.compile("\\s");

	/** mySocket je socket, o který se bude tento SocketHandler starat */
	Socket mySocket;

	/** client ID je øetìzec ve formátu <IP_adresa>:<port> */
	String clientID;
	volatile String userName;
	final Set<String> groups = ConcurrentHashMap.newKeySet();

	/**
	 * activeHandlers je reference na mnoinu vech právì bìících SocketHandlerù.
	 * Potøebujeme si ji udrovat, abychom mohli zprávu od tohoto klienta
	 * poslat vem ostatním!
	 */
	ActiveHandlers activeHandlers;

	/**
	 * messages je fronta pøíchozích zpráv, kterou musí mít kaý klient svoji
	 * vlastní - pokud bude je pøetíená nebo nefunkèní klientova sí,
	 * èekají zprávy na doruèení právì ve frontì messages
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);

	/**
	 * startSignal je synchronizaèní závora, která zaøizuje, aby oba tasky
	 * OutputHandler.run() a InputHandler.run() zaèaly ve stejný okamik.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler.run() se bude starat o OutputStream mého socketu */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler.run() se bude starat o InputStream mého socketu */
	InputHandler inputHandler = new InputHandler();
	/**
	 * protoe v outputHandleru nedovedu detekovat uzavøení socketu, pomùe mi
	 * inputFinished
	 */
	volatile boolean inputFinished = false;

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
	}

	class OutputHandler implements Runnable {
		public void run() {
			try (OutputStreamWriter writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8")) {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer.write("\nYou are connected from " + clientID + "\n");
				writer.write("Please enter your name (one word), or use #setMyName <name>:\n");
				writer.flush();
				while (!inputFinished || !messages.isEmpty()) {
					String message = messages.poll(250, TimeUnit.MILLISECONDS);
					if (message != null) {
						writer.write(message + "\r\n");
						writer.flush();
						System.err.println("DBG>Message sent to " + clientID + ":" + message + "\n");
					}
				}
			} catch (IOException e) {
				if (!inputFinished) {
					e.printStackTrace();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			try {
				mySocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");
		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				String request;
				while ((request = reader.readLine()) != null) {
					handleRequest(request);
				}
			} catch (IOException e) {
				if (!mySocket.isClosed()) {
					e.printStackTrace();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				inputFinished = true;
				activeHandlers.remove(SocketHandler.this);
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

	}

	private void handleRequest(String request) {
		if (userName == null) {
			if (request.startsWith("#setMyName")) {
				handleCommand(request);
				return;
			}

			String requestedName = request.trim();
			if (!isValidToken(requestedName) || requestedName.startsWith("#")) {
				sendReply("Please enter a valid name without spaces.");
			} else if (!activeHandlers.setName(this, requestedName)) {
				sendReply("That name is already in use. Please enter another name.");
			}
			return;
		}

		if (request.startsWith("#")) {
			handleCommand(request);
			return;
		}
		String message = "[" + userName + "] >> " + request;
		System.out.println(message);
		activeHandlers.sendGroupMessage(this, message);
	}

	private void handleCommand(String request) {
		String[] parts = request.trim().split("\\s+", 3);
		switch (parts[0]) {
			case "#setMyName":
				if (parts.length != 2 || !isValidToken(parts[1])) {
					sendReply("Usage: #setMyName <name>");
				} else if (!activeHandlers.setName(this, parts[1])) {
					sendReply("That name is already in use.");
				} else {
					System.out.println("Client " + clientID + " is now known as " + userName);
				}
				break;
			case "#sendPrivate":
				if (parts.length != 3 || !isValidToken(parts[1]) || parts[2].isEmpty()) {
					sendReply("Usage: #sendPrivate <name> <message>");
				} else if (userName == null) {
					sendReply("Please set your name first using #setMyName <name>.");
				} else if (!activeHandlers.sendPrivateMessage(this, parts[1],
						"[" + userName + "] >> " + parts[2])) {
					sendReply("User " + parts[1] + " was not found.");
				}
				break;
			case "#join":
				if (parts.length != 2 || !isValidToken(parts[1])) {
					sendReply("Usage: #join <name>");
				} else {
					activeHandlers.joinGroup(this, parts[1]);
				}
				break;
			case "#leave":
				if (parts.length != 2 || !isValidToken(parts[1])) {
					sendReply("Usage: #leave <name>");
				} else if (!activeHandlers.leaveGroup(this, parts[1])) {
					sendReply("You are not in group " + parts[1] + ".");
				}
				break;
			case "#groups":
				if (parts.length != 1) {
					sendReply("Usage: #groups");
				} else {
					sendReply(activeHandlers.getGroups(this));
				}
				break;
			default:
				sendReply("Unknown command.");
		}
	}

	private boolean isValidToken(String token) {
		return !token.isEmpty() && !WHITESPACE.matcher(token).find();
	}

	private void sendReply(String message) {
		if (!messages.offer(message)) {
			System.err.printf("Client %s message queue is full, dropping the message!%n", clientID);
		}
	}
}

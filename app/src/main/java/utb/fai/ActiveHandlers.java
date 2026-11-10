package utb.fai;

import java.util.*;

public class ActiveHandlers {
    private static final String PUBLIC_GROUP = "public";

    private final Set<SocketHandler> activeHandlersSet = new HashSet<>();
    private final Map<String, Set<SocketHandler>> groups = new HashMap<>();
    private final int maxConnections;

    ActiveHandlers(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    synchronized boolean add(SocketHandler handler) {
        if (activeHandlersSet.size() >= maxConnections) {
            return false;
        }
        if (!activeHandlersSet.add(handler)) {
            return false;
        }
        joinGroup(handler, PUBLIC_GROUP);
        return true;
    }

    synchronized boolean remove(SocketHandler handler) {
        if (!activeHandlersSet.remove(handler)) {
            return false;
        }
        for (String group : new HashSet<>(handler.groups)) {
            leaveGroup(handler, group);
        }
        return true;
    }

    synchronized boolean setName(SocketHandler handler, String name) {
        for (SocketHandler activeHandler : activeHandlersSet) {
            if (activeHandler != handler && name.equals(activeHandler.userName)) {
                return false;
            }
        }
        handler.userName = name;
        return true;
    }

    synchronized void sendGroupMessage(SocketHandler sender, String message) {
        Set<SocketHandler> recipients = new HashSet<>();
        for (String group : sender.groups) {
            Set<SocketHandler> members = groups.get(group);
            if (members != null) {
                recipients.addAll(members);
            }
        }
        recipients.remove(sender);
        sendToHandlers(recipients, message);
    }

    synchronized boolean sendPrivateMessage(SocketHandler sender, String recipientName, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (recipientName.equals(handler.userName)) {
                sendToHandler(handler, message);
                return true;
            }
        }
        return false;
    }

    synchronized boolean joinGroup(SocketHandler handler, String group) {
        if (!activeHandlersSet.contains(handler)) {
            return false;
        }
        handler.groups.add(group);
        groups.computeIfAbsent(group, ignored -> new HashSet<>()).add(handler);
        return true;
    }

    synchronized boolean leaveGroup(SocketHandler handler, String group) {
        if (!handler.groups.remove(group)) {
            return false;
        }
        Set<SocketHandler> members = groups.get(group);
        if (members != null) {
            members.remove(handler);
            if (members.isEmpty()) {
                groups.remove(group);
            }
        }
        return true;
    }

    synchronized String getGroups(SocketHandler handler) {
        List<String> clientGroups = new ArrayList<>(handler.groups);
        Collections.sort(clientGroups);
        return String.join(",", clientGroups);
    }

    private void sendToHandlers(Collection<SocketHandler> handlers, String message) {
        for (SocketHandler handler : handlers) {
            sendToHandler(handler, message);
        }
    }

    private void sendToHandler(SocketHandler handler, String message) {
        if (!handler.messages.offer(message)) {
            System.err.printf("Client %s message queue is full, dropping the message!%n", handler.clientID);
        }
    }
}

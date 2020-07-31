package com.anmol.projects;

import java.util.*;

public class ConsistentHash<T, R> {

    private final int numberOfReplicas;
    private final TreeMap<Integer, T> ring = new TreeMap<Integer, T>();
    private final List<HashFunction<T>> hashFunctions = new ArrayList<HashFunction<T>>();
    private final HashFunction<R> requestFunctionHash;
    private final int ringSize;

    public ConsistentHash(int numberOfReplicas, int ringSize, Random random) {
        this.numberOfReplicas = numberOfReplicas;
        this.ringSize = ringSize;
        // create number of hash functions
        for (int i = 0; i < numberOfReplicas; i++) {
            int randomNumber = Math.abs(random.nextInt(1000));
            hashFunctions.add(new HashFunction<T>(randomNumber));
        }
        int requestRandomNumber = Math.abs(random.nextInt(1000));
        requestFunctionHash = new HashFunction<R>(requestRandomNumber);

    }

    public ConsistentHash(int numberOfReplicas, int ringSize) {
        this(numberOfReplicas, ringSize, new Random());

    }

    public void addServer(T server) {

        for (int i = 0; i < numberOfReplicas; i++) {
            HashFunction<T> oneHashFunction = hashFunctions.get(i);
            Integer serverHash = oneHashFunction.getHash(server) % ringSize;
            ring.put(serverHash, server);
        }

    }

    public void removeServer(T server) {
        for (int i = 0; i < numberOfReplicas; i++) {
            HashFunction<T> oneHashFunction = hashFunctions.get(i);
            Integer serverHash = oneHashFunction.getHash(server) % ringSize;
            if (ring.get(serverHash) == server) {
                ring.remove(serverHash);
            }

        }
    }

    public T getServerForRequest(R request) {
        Integer requestHash = (requestFunctionHash.getHash(request) % ringSize);
        SortedMap<Integer, T> serversAfterThat = ring.tailMap(requestHash);
        Integer serverKey = null;
        if (serversAfterThat.isEmpty()) {
            serverKey = ring.firstKey();
        } else {
            serverKey = serversAfterThat.firstKey();
        }
        return ring.get(serverKey);
    }

    public TreeMap<Integer, T> getRing() {
        return ring;
    }

    @Override
    public String toString() {
        return "ConsistentHash{" +
                "numberOfReplicas=" + numberOfReplicas +
                ", ringSize=" + ringSize +
                '}';
    }
}

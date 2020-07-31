package com.anmol.projects;

import org.junit.Test;

import java.util.*;

public class ConsistentHashTestToCheckDistribution {
    class Stats {
        double servedBefore, servedAfter, requestsMovedToThisServer, requestsMovedFromThisServer;
    }

    @Test
    public void Should_DistributeRequestLoadToServers_When_OneServerIsRemoved() {
        Random random = new Random();
        random.setSeed(10000);
        ConsistentHash<String, String> consistentHash = new ConsistentHash<String, String>(100, 1000,random);
        int numberOfServers = 5;
        List<String> serverIds = new ArrayList<>();
        for (int i = 0; i < numberOfServers; i++) {
            String serverName = "server" + i;
            consistentHash.addServer(serverName);
            serverIds.add(serverName);
        }

        System.out.println(consistentHash);
        // generate requests
        int generatedRequests = 1000;
        List<String> requests = generateRequests(generatedRequests,random);
        Map<String, String> before = new HashMap<>();
        for (String request : requests) {
            before.put(request, consistentHash.getServerForRequest(request));
        }
        consistentHash.removeServer(serverIds.get(0));
        Map<String, String> after = new HashMap<>();
        for (String request : requests) {
            after.put(request, consistentHash.getServerForRequest(request));
        }

        printStatsInformation(before, after, serverIds,generatedRequests);


    }

   private double r2(double a){
        return Math.round(a * 100.0) / 100.0;
   }

    private String l(Object o){
        String s = o.toString();
        int pad =5;

        return String.format("%"+pad+"s",s);
    }

    private void printStatsInformation(Map<String, String> before, Map<String, String> after,
                                       List<String> serverIds, int generatedRequests) {
        double divider = 100/(double)generatedRequests;
        Map<String, Stats> serverStats = new HashMap<>();
        for (String serverName : serverIds) {
            serverStats.put(serverName, new Stats());
        }
        for(String request:before.keySet()){
            String beforeServer = before.get(request);
            String afterServer = after.get(request);
            serverStats.get(beforeServer).servedBefore +=1;
            serverStats.get(afterServer).servedAfter +=1;
            if(!beforeServer.equals(afterServer)){
                serverStats.get(beforeServer).requestsMovedFromThisServer+=1;
                serverStats.get(afterServer).requestsMovedToThisServer+=1;
            }

        }

        System.out.println("\t"+l("Id")+"\t"+l("before")+"\t"+l("after")+"\t"+l("mFrom")+"\t"+l("mTo"));
        for(String key: serverStats.keySet()){
            Stats stats = serverStats.get(key);
            System.out.println("\t"+l(key)+
                    "\t"+l(r2(stats.servedBefore*divider))+
                    "\t"+l(r2(stats.servedAfter*divider))+
                    "\t"+l(r2(stats.requestsMovedFromThisServer*divider))+
                    "\t"+l(r2(stats.requestsMovedToThisServer*divider)));
        }

    }


    private void print(Map<String, String> map) {
        for (String key : map.keySet()) {
            System.out.println("\t" + key + "\t" + map.get(key));
        }
    }

    private List<String> generateRequests(int n, Random random) {
        List<String> requests = new ArrayList<>();
        for (int i = 0; i < n; i++) {
//            requests.add(UUID.randomUUID().toString());
            requests.add(Math.abs(random.nextLong())+"-"+Math.abs(random.nextLong()));

        }
        return requests;
    }
}

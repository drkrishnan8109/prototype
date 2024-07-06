package org.random;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/*
* Logic is that we have constant size array for holding the Caches or Nodes.
* When a Cache goes down, the index is cleared, & by default the key search goes to the right index in the ring
* The key redistribution happens only for the keys held by that cache. All of it goes to the right node
*
* For hot shard, manually add a new cache in the ring array. It boots up with a snapshot of the hot shard on the right.
* Eventually delete unwanted keys.
* */
public class ConsistentHashing {

    int n = 2;
    ArrayList<HashMap<String,String>> hashring = new ArrayList<>();

    void addCacheToRingDefault(HashMap<String,String> cache1, HashMap<String,String> cache2) {
        //Add initial x number of nodes
        //hash based decision
        int index1 = hash(cache1);
        int index2 = hash(cache2);
        //Add the caches based on sorted hash into the ring array
        if(index1<index2) {
            hashring.set(0, cache1);
            hashring.set(1, cache2);
        }
        else {
            hashring.set(0, cache2);
            hashring.set(1, cache1);
        }
    }

    void addKey(String key) {
        //find cache index
        int index = hash(key);
        //Find valid index - if cache went down, find next cache
        while(hashring.get(index) ==null) {
            index = index++ >= n ? 0 : index;
        }
        HashMap cache = hashring.get(index); // Key will be added to this cache
        cache.put(key,"blabla"); //Ofcourse just a prototype!
    }

    public String getKey(String key) {
        int index = hash(key);
        //Find valid index - if cache went down, find next cache
        int limit = 0;
        while(hashring.get(index) ==null) {
            index = index++ >= n ? 0 : index;
            limit++; if(limit>n) return null; // To prevent infinite loop
        }
        HashMap cache = hashring.get(index);
        return (String) cache.get(key);
    }

    void addMoreCacheForHotShard(int breakindex) {
        //Decide manually which the index in ring should the cache be added & manually move array to make space for new node
        //Load with snapshot of hot shard
        HashMap<String,String> newCache = (HashMap<String, String>) hashring.get(breakindex).clone();
        int newIndex = hash(newCache);
        if(hashring.get(newIndex)!=null) {
            //Insert to array index
            hashring.add(newIndex, newCache);
        }
        else {
            // Set to array index
            hashring.set(newIndex,newCache);
            //Later delete unwanted keys to correct the key range
        }

    }

    int hash(String key) {
        int k=0;
        for (int i=0; i<key.length();i++) {
            k+=key.charAt(i);
        }
        return k%n;
    }

    int hash(HashMap cache) {
        int k=0;
        String bla = cache.toString(); // Just a naiveway to understand logic, dont iterate like this is real world!
        return hash(bla);
    }

}

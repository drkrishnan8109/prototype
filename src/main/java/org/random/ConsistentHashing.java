package org.random;

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

    static int n = 10; //hard code for prototype
    HashMap<String,String>[] hashring = new HashMap[10];

    /*
    * Add initial x number of nodes
    * with constant n
    * */
    public void assignCacheToRingDefault(HashMap<String,String> cache1, HashMap<String,String> cache2) {
        int index1 = 4;
        int index2 = 8;
        System.out.println("Adding cache at cache index " + index1);
        hashring[index1] = cache1;
        System.out.println("Adding cache at cache index " + index2);
        hashring[index2] = cache2;
    }

    /*
    * If success, return key, else returns null
    * */
    public String put(String key, String value) {
        //find cache index
        int index = Integer.parseInt(key)%n;
        int limit=0;
        //Find valid index where cache is present
        index = index >= n ? 0 : index;
        while(hashring[index] ==null) {
            index = ++index >= n ? 0 : index; //Infinite loop if all caches down. Need to handle such edge cases - Set alerts
            limit++; if(limit>n) return null; // To prevent infinite loop and to inform caller that add failed
        }
        System.out.println("Putting key" + key +":"+ index);
        HashMap cache = hashring[index]; // Key will be added to this cache
        cache.put(key,value); //Ofcourse just a prototype!
        return key;
    }

    public String get(String key) {
        int index = Integer.parseInt(key)%n;
        //Find valid index - if cache went down, find next cache
        int limit = 0;
        index = index >= n ? 0 : index;
        while(hashring[index] ==null) {
            index = ++index >= n ? 0 : index;
            limit++; if(limit>n) return null; // To prevent infinite loop
        }
        System.out.println("Getting key" + key +":"+ index);
        HashMap cache = hashring[index];
        return (String) cache.get(key);
    }

    /*
    * Cache failures and Hotshards needs to be handled
    * Hotshard: Here I chose manual splitting of shard that is hot - Find such shards with KPIs for load & alert
    * Cache failure: Failure KPIs should give us the index of failed shard
    * Not using the virtual node approach here, rather going for manual to prototype
    * For hotshards breakIndex is the index of hot shard
    * For cache failure breakIndex is Index of next available cache
     * */
    public void addMoreCache(int newIndex) {
        //Decide manually which the index in ring should the cache be added & manually move array to make space for new node
        //Load with snapshot of hot shard
        int nextValidCacheIndex = newIndex + 1;
        while(hashring[nextValidCacheIndex] ==null) {
            nextValidCacheIndex = ++nextValidCacheIndex >= n ? 0 : nextValidCacheIndex;
        }
        HashMap<String, String> newCache = (HashMap<String, String>)  hashring[nextValidCacheIndex].clone();
        // Insert new cache to array at the given index
        System.out.println("Adding more cache at index "+ newIndex);
        hashring[newIndex] = newCache; //Right now newcache and cloned cache both has same data
        //Later rebalance keys to correct the key range
        int prevValidCacheIndex = newIndex;
        while(hashring[prevValidCacheIndex] ==null) {
            prevValidCacheIndex = --prevValidCacheIndex < 0 ? n : prevValidCacheIndex;
        }
        rebalanceKeys(prevValidCacheIndex, nextValidCacheIndex, newIndex);
        }

    public void rebalanceKeys(int prevValidCacheIndex, int nextValidCacheIndex, int currIndex) {
        //Iterate through cache at prevValidCacheIndex and assign keys in range [prevValidCacheIndex,currIndex] to cache at currIndex
        //Iterate through cache at nextValidCacheIndex and assign keys in range [currIndex,nextValidCacheIndex] to cache at currIndex
        //Also delete those keys from the old cache
        //Ignore concurrency for now
    }
}

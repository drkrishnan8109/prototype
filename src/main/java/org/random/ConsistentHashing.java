package org.random;

import org.w3c.dom.ranges.Range;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
    HashMap<Integer,Integer>[] hashring = new HashMap[10];

    /*
    * Add initial x number of nodes
    * with constant n
    * */
    public void assignCacheToRingDefault(HashMap<Integer,Integer> cache1, HashMap<Integer,Integer> cache2) {
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
    public Integer put(Integer key, Integer value) {
        //find cache index
        int index =key%n;
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

    public Integer get(Integer key) {
        int index = key%n;
        //Find valid index - if cache went down, find next cache
        int limit = 0;
        index = index >= n ? 0 : index;
        while(hashring[index] ==null) {
            index = ++index >= n ? 0 : index;
            limit++; if(limit>n) return null; // To prevent infinite loop
        }
        System.out.println("Getting key" + key +":"+ index);
        HashMap cache = hashring[index];
        return (Integer) cache.get(key);
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
        HashMap<Integer, Integer> newCache = (HashMap<Integer, Integer>)  hashring[nextValidCacheIndex].clone();
        // Insert new cache to array at the given index
        System.out.println("Adding more cache at index "+ newIndex);
        hashring[newIndex] = newCache; //Right now newcache and cloned cache both has same data
        //Later rebalance keys to correct the key range
        int prevValidCacheIndex = newIndex;
        while(hashring[prevValidCacheIndex] ==null) {
            prevValidCacheIndex = --prevValidCacheIndex < 0 ? n : prevValidCacheIndex;
        }
        rebalanceKeysonNewCache(prevValidCacheIndex, nextValidCacheIndex, newIndex);
        }

    public void rebalanceKeysonNewCache(int prevValidCacheIndex, int nextValidCacheIndex, int currIndex) {
        //Ignore concurrency for now
        HashMap<Integer,Integer> nextCache = hashring[nextValidCacheIndex];
        HashMap<Integer,Integer> currCache = hashring[currIndex];

        // Assign keys in range [prevValidCacheIndex,currIndex] to cache at currIndex
        // and remove those keys from cache at nextValidCacheIndex
        for(Map.Entry<Integer,Integer> entry: nextCache.entrySet()) {
            if( entry.getKey()>prevValidCacheIndex && entry.getKey()<=currIndex ) {
                currCache.put(entry.getKey(),entry.getValue());
                nextCache.remove(entry.getKey());
            }
        }
    }

    /*
    * Assuming cache at failIndex goes down.
    * Either all data is lost if the cache was only memory, in that case the cache is eventually rebuilt by disk reads
    * Or we had snapshots or WAL and we copy all the data to next shard, if the shard is overwhelmed, split it using addMoreCache()
    * */
    public void gracefullyFailCache(int failIndex) {

    }
}

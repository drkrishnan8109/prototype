package org.random;

/*
* No dream is too big! :)
* Step by Step or Byte by Byte
*
* Write data to append only logfile in the format: key_size, value_size, key, value
* Hence we know number of bytes of key and number of bytes of value to read, and read accordingly from the file
*
* Index - use In-memory HashMap of key,offset
* After every x bytes wrote, update index ? But then index could serve stale data
* */
public class BitCaskProto {
}

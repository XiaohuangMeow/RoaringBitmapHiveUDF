package org.example;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();

        bitmap1.addRange(-5L,5L);
        bitmap1.getLongCardinality();
        ////        bitmap1.serialize(ByteBuffer.wrap(bytes));
////        bitmap1.deserialize(ByteBuffer.wrap(bytes));
//        System.out.println(bitmap1);

    }
}
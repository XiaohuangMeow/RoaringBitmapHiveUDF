package org.example;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) throws IOException {
//        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
//        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();

//        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
//        Roaring64Bitmap bitmap2=new Roaring64Bitmap();
//        System.out.println(Long.MIN_VALUE);
//        System.out.println(Long.MAX_VALUE);
        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();
        long cnt=0;
        for (long i = (long) -1e9;  i<1e9; i+=500) {
            if (cnt%100000==0){
                System.out.println(cnt);
            }
            cnt++;
            bitmap1.add(i*3);
        }
        for (long i = (long) -1e9;  i<1e9; i+=1000) {
            if (cnt%100000==0){
                System.out.println(cnt);
            }
            cnt++;
            bitmap2.add(i*10);
        }

        bitmap1.xor(bitmap2);
        System.out.println(bitmap1.serializedSizeInBytes());
        bitmap1.runOptimize();
        System.out.println(bitmap1.serializedSizeInBytes());
        System.out.println(bitmap1.getLongCardinality());

        ////        bitmap1.serialize(ByteBuffer.wrap(bytes));
////        bitmap1.deserialize(ByteBuffer.wrap(bytes));
//        System.out.println(bitmap1);

    }
}
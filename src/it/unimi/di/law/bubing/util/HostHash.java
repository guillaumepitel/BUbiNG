package it.unimi.di.law.bubing.util;
//RELEASE-STATUS: DIST

import it.unimi.di.law.bubing.util.MurmurHash3;
import it.unimi.di.law.bubing.util.MurmurHash3_128;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HostHash {
    private static final Charset charset = StandardCharsets.UTF_8;

    public static long hostLongHash(String host) {
        return (MurmurHash3_128.murmurhash3_x86_32(host,0,host.length(),8899));
    }
    public static long hostLongHash(String host, int offset, int len) {
        return (MurmurHash3_128.murmurhash3_x86_32(host,offset,len,8899));
    }
}

package it.unimi.di.law.bubing.util;
//RELEASE-STATUS: DIST

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HostHash {
    private static final Charset charset = StandardCharsets.UTF_8;

    public static final long hostLongHash(final String host) {
        return (MurmurHash3_128.murmurhash3_x86_32(host,0,host.length(),8899));
    }
    public static final long hostLongHash(final String host, final int offset, final int len) {
        return (MurmurHash3_128.murmurhash3_x86_32(host,offset,len,8899));
    }
}

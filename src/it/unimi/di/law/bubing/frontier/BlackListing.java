package it.unimi.di.law.bubing.frontier;

//RELEASE-STATUS: DIST

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.locks.Lock;

import static it.unimi.di.law.bubing.util.HostHash.hostLongHash;

public class BlackListing {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlackListing.class);

    public static boolean checkBlacklistedIP(Frontier frontier, URI url, byte[] address)
    {
        boolean blackListed = false;
        if (address.length != 4)
            return false; // We check only IPv4 addresses
        Lock lock = frontier.rc.blackListedIPv4Lock.readLock();
        lock.lock();

        try {
            // We use Ints.fromBytes() as the array version generates an Object array just to log, possibly, a wrong argument (!)
            if (frontier.rc.blackListedIPv4Addresses.contains(Ints.fromBytes(address[0], address[1], address[2], address[3]))) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("URL {} disallowed by last-minute check for IP blacklisting", url);
                blackListed = true;
            }
        } catch (Exception e) {
            LOGGER.warn("Exception in blacklist Ip checking {}", url, e);
        } finally {
            lock.unlock();
        }
        return blackListed;
    }

    public static boolean checkBlacklistedHost(Frontier frontier, URI url)
    {
        String host = url.getHost();
        int hostlen = host.length();
        int hoststart = 0;
        boolean blackListed = false;
        Lock lock = frontier.rc.blackListedHostHashesLock.readLock();
        lock.lock();
        try {
               do {
                   if (frontier.rc.blackListedHostHashes.contains(hostLongHash(host, hoststart, hostlen - hoststart))) {
                       if (LOGGER.isDebugEnabled())
                           LOGGER.debug("URL {} disallowed by last-minute check for Host blacklisting", url);
                       return true;
                   }
                   hoststart = host.indexOf('.', hoststart) + 1;
               } while ( hoststart != -1 && host.indexOf('.', hoststart) != -1);
        } catch (Exception e) {
            LOGGER.warn("Exception in blacklist Host checking {}", host, e);
        } finally {
            lock.unlock();
        }
        return blackListed;
    }
}

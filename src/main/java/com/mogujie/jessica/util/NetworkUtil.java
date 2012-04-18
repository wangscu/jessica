package com.mogujie.jessica.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.log4j.Logger;

/**
 * @project:杭州卷瓜网络有限公司搜索引擎
 * @date:2011-9-19
 * @author:xuanxi
 */
public class NetworkUtil
{
    private static final Logger log = Logger.getLogger(NetworkUtil.class);

    /**
     * 获得一个非localhost的网络ip
     */
    public static Inet4Address getIpAddress()
    {
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface netint : Collections.list(nets))
            {
                if (netint.getName().equals("lo"))
                {
                    continue;
                }
                Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddresses))
                {
                    if (inetAddress instanceof Inet4Address)
                    {
                        return (Inet4Address) inetAddress;
                    }
                }
            }
        } catch (Exception e)
        {
            log.error("获得一个非localhost的网络ip!" + e.getMessage(), e);
        }
        return null;
    }

    public static Inet4Address getIpAddress(String networkInterfaceName)
    {
        try
        {
            NetworkInterface networkInterface = NetworkInterface.getByName(networkInterfaceName);
            if (networkInterface == null)
            {
                return null;
            }

            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            for (InetAddress inetAddress : Collections.list(inetAddresses))
            {
                if (inetAddress instanceof Inet4Address)
                {
                    return (Inet4Address) inetAddress;
                }
            }
        } catch (Exception e)
        {
            log.error("通过指定网卡" + networkInterfaceName + "获得对应的ip地址失败!" + e.getMessage(), e);
        }
        return null;
    }

}

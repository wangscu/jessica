package com.mogujie.storeage;

import java.util.List;
import java.util.Map;

import com.mogujie.jessica.service.thrift.Doc;

/**
 * @project:杭州卷瓜有限公司搜索引擎
 * @date:2011-9-19
 * @author:xuanxi
 */
public interface Store
{
    public void start();

    public void shutdown();

    public int getStatus();

    public void setStatus(int status);

    public int getDocSize();

    public void setCacheSize(int size);

    public int getCacheSize();

    public int capacity();

    public int queueSize();

    public long diskSize();

    public void clearCache();

    public long getEviction();

    public String getStorePath();

    public void setBlockSize(long size);

    public Map<String, String> get(Long key);

    public void put(Long key, Map<String, String> data);

    public void putAll(List<Doc> docs);

    public void merge();

}

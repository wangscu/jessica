package com.mogujie.jessica;

import java.io.File;
import java.net.Inet4Address;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mogujie.jessica.index.IndexWriter;
import com.mogujie.jessica.journal.Journal;
import com.mogujie.jessica.service.AdminServiceImpl;
import com.mogujie.jessica.service.SearchServiceImpl;
import com.mogujie.jessica.service.UpdateServiceImpl;
import com.mogujie.jessica.util.NetworkUtil;
import com.mogujie.storeage.SimpleBitcaskStore;


/**
 * 加一个非deamon线程 防止程序空闲的时候 自动退出
 * 
 * @author xuanxi
 * 
 */
public class MSearcher
{
    private final static Logger logger = Logger.getLogger(MSearcher.class);
    private boolean isRecover = false;
    private IndexWriter indexWriter;
    private Journal journal;
    private UpdateServiceImpl updateService;
    private SearchServiceImpl searchService;
    private AdminServiceImpl adminService;
    private SimpleBitcaskStore bitcaskStore;

    public SearchServiceImpl getSearchService()
    {
        return searchService;
    }

    public void setSearchService(SearchServiceImpl searchService)
    {
        this.searchService = searchService;
    }

    private static MSearcher instance = new MSearcher();

    public static MSearcher getInstance()
    {
        return instance;
    }

    public static void main(String[] args)
    {
        try
        {
            // step 1,解析配置文件获得配置参数
            Properties properties = new Properties();
            properties.load(MSearcher.class.getClassLoader().getResourceAsStream("conf/jessica.properties"));

            Inet4Address inet4Address = NetworkUtil.getIpAddress("eth1");
            String defaultIp = (inet4Address == null) ? "127.0.0.1" : inet4Address.getHostAddress();
            String defaultAdminPort = "8723";
            String defaultUpdatePort = "8724";
            String defaultSearchPort = "8725";
            String defaultStoreDir = "/var/data/jessica/store/";
            String defaultJournalDir = "/var/data/jessica/journal/";
            String defaultJournalRecover = "false";

            String ip = properties.getProperty("ip", defaultIp);
            String adminPort = properties.getProperty("admin.port", defaultAdminPort);
            String updatePort = properties.getProperty("update.port", defaultUpdatePort);
            String searchPort = properties.getProperty("search.port", defaultSearchPort);
            String storeDir = properties.getProperty("store.dir", defaultStoreDir);
            String journalDir = properties.getProperty("journal.dir", defaultJournalDir);
            String journalRecover = properties.getProperty("journal.recover", defaultJournalRecover);

            final MSearcher ms = MSearcher.getInstance();
            final IndexWriter indexWriter = new IndexWriter();
            final SimpleBitcaskStore bitcaskStore = new SimpleBitcaskStore(new File(storeDir));
            final Journal journal = new Journal(new File(journalDir));

            // step 2,创建thrift 网络接口
            final UpdateServiceImpl updateService = new UpdateServiceImpl(indexWriter, bitcaskStore, journal, ip, Integer.parseInt(updatePort));
            final SearchServiceImpl searchService = new SearchServiceImpl(indexWriter, bitcaskStore, ip, Integer.parseInt(searchPort));
            final AdminServiceImpl adminService = new AdminServiceImpl(ms, bitcaskStore, ip, Integer.parseInt(adminPort));

            ms.setIndexWriter(indexWriter);
            ms.setUpdateService(updateService);
            ms.setSearchService(searchService);
            ms.setAdminService(adminService);
            ms.setBitcaskStore(bitcaskStore);
            ms.setJournal(journal);

            if (journalRecover.trim().equalsIgnoreCase("true"))
            {
                ms.setRecover(true);
            } else
            {
                ms.setRecover(false);
            }

            // step 3,设置关闭函数
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        ms.shutdown();
                    } catch (Exception e)
                    {
                        logger.error(e.getMessage(), e);
                        System.err.print(e.getMessage());
                    }
                }
            }));

            // step 4,启动服务器
            ms.start();
        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
            System.err.print(e.getMessage());
        }
    }

    public void start() throws Exception
    {
        indexWriter.start();
        bitcaskStore.start();
        journal.start();
        if (isRecover)
        {
            // 从日志中恢复索引数据
            journal.recover(indexWriter);
        }
        updateService.start();
        searchService.start();
        adminService.start();

    }

    public void shutdown() throws Exception
    {
        indexWriter.shutdown();
        bitcaskStore.shutdown();
        journal.shutdown();
        updateService.shutdown();
        searchService.shutdown();
        adminService.shutdown();
    }

    public IndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    public void setIndexWriter(IndexWriter indexWriter)
    {
        this.indexWriter = indexWriter;
    }

    public UpdateServiceImpl getUpdateService()
    {
        return updateService;
    }

    public void setUpdateService(UpdateServiceImpl updateService)
    {
        this.updateService = updateService;
    }

    public AdminServiceImpl getAdminService()
    {
        return adminService;
    }

    public void setAdminService(AdminServiceImpl adminService)
    {
        this.adminService = adminService;
    }

    public SimpleBitcaskStore getBitcaskStore()
    {
        return bitcaskStore;
    }

    public void setBitcaskStore(SimpleBitcaskStore bitcaskStore)
    {
        this.bitcaskStore = bitcaskStore;
    }

    public Journal getJournal()
    {
        return journal;
    }

    public void setJournal(Journal journal)
    {
        this.journal = journal;
    }

    public boolean isRecover()
    {
        return isRecover;
    }

    public void setRecover(boolean isRecover)
    {
        this.isRecover = isRecover;
    }

}

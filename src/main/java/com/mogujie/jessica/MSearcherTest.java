//package com.mogujie.jessica;
//
//import java.io.File;
//import java.net.Inet4Address;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.log4j.Logger;
//
//import com.mogujie.jessica.MSearcher;
//import com.mogujie.jessica.index.MIndexWriter;
//import com.mogujie.jessica.journal.Journal;
//import com.mogujie.jessica.service.AdminServiceImpl;
//import com.mogujie.jessica.service.SearchServiceImpl;
//import com.mogujie.jessica.service.UpdateServiceImpl;
//import com.mogujie.jessica.service.thrift.Doc;
//import com.mogujie.jessica.service.thrift.ResultHit;
//import com.mogujie.jessica.service.thrift.SearchRequest;
//import com.mogujie.jessica.service.thrift.SearchResponse;
//import com.mogujie.jessica.service.thrift.TDocument;
//import com.mogujie.jessica.service.thrift.TField;
//import com.mogujie.jessica.service.thrift.TToken;
//import com.mogujie.jessica.util.NetworkUtil;
//import com.mogujie.storeage.SimpleBitcaskStore;
//
//public class MSearcherTest
//{
//    private final static Logger logger = Logger.getLogger(MSearcher.class);
//
//    public static void main(String[] args)
//    {
//        // range("status", 1, 1);
//        // range("status", 2, 2);
//        // range("status", 1, 2);
//        // and("content", "装", "女");
//        // or("status", "1", "2");
//        not("content", "装", "女");
//        for (int i = 1; i < 1000; i++)
//        {
//            // e("ddd", "xxx", i);
//        }
//
//    }
//
//    public static void not(String field, String term1, String term2)
//    {
//        try
//        {
//            // step 1,解析配置文件获得配置参数
//            Properties properties = new Properties();
//            properties.load(MSearcher.class.getClassLoader().getResourceAsStream("conf/jessica.properties"));
//
//            Inet4Address inet4Address = NetworkUtil.getIpAddress("eth1");
//            String defaultIp = (inet4Address == null) ? "127.0.0.1" : inet4Address.getHostAddress();
//            String defaultAdminPort = "8723";
//            String defaultUpdatePort = "8724";
//            String defaultSearchPort = "8725";
//            String defaultStoreDir = "/var/data/jessica/store/";
//            String defaultJournalDir = "/var/data/jessica/journal/";
//            String defaultJournalRecover = "false";
//
//            String ip = properties.getProperty("ip", defaultIp);
//            String adminPort = properties.getProperty("admin.port", defaultAdminPort);
//            String updatePort = properties.getProperty("update.port", defaultUpdatePort);
//            String searchPort = properties.getProperty("search.port", defaultSearchPort);
//            String storeDir = properties.getProperty("store.dir", defaultStoreDir);
//            String journalDir = properties.getProperty("journal.dir", defaultJournalDir);
//            String journalRecover = properties.getProperty("journal.recover", defaultJournalRecover);
//
//            final MSearcher ms = MSearcher.getInstance();
//            final MIndexWriter indexWriter = new MIndexWriter();
//            final SimpleBitcaskStore bitcaskStore = new SimpleBitcaskStore(new File(storeDir));
//            final Journal journal = new Journal(new File(journalDir));
//
//            // step 2,创建thrift 网络接口
//            final UpdateServiceImpl updateService = new UpdateServiceImpl(indexWriter, bitcaskStore, journal, ip, Integer.parseInt(updatePort));
//            final SearchServiceImpl searchService = new SearchServiceImpl(indexWriter, bitcaskStore, ip, Integer.parseInt(searchPort));
//            final AdminServiceImpl adminService = new AdminServiceImpl(ms, bitcaskStore, ip, Integer.parseInt(adminPort));
//
//            ms.setIndexWriter(indexWriter);
//            ms.setUpdateService(updateService);
//            ms.setSearchService(searchService);
//            ms.setAdminService(adminService);
//            ms.setBitcaskStore(bitcaskStore);
//            ms.setJournal(journal);
//            ms.setRecover(true);
//
//            // step 4,启动服务器
//            ms.start();
//
//            // step 6,query
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.sortReverse = true;
//            searchRequest.offset = 0;
//            searchRequest.limit = 100000;
//            String _term1 = "{'type':'TermQuery','field':'" + field + "','term':'" + term1 + "'}";
//            String _term2 = "{'type':'TermQuery','field':'" + field + "','term':'" + term2 + "'}";
//            searchRequest.query = "{'type':'DifferenceQuery','leftNode':" + _term1 + ",'rightNode':" + _term2 + "}";
//            Map<String, Integer> sortMap = new HashMap<String, Integer>();
//            sortMap.put("timeWeight", 1000);
//            sortMap.put("favWeight", 1000);
//            sortMap.put("editorWeight", 1000);
//            sortMap.put("maxFav", 1000);
//            sortMap.put("maxEditor", 1000);
//            searchRequest.sortMap = sortMap;
//            searchRequest.qtime = 300000;
//            SearchResponse response = searchService.search(searchRequest);
//            List<ResultHit> rs = response.getResults();
//            // step 7,check feed and query
//            logger.error("hits:" + response.getHits());
//
//            // step 8, shutdown
//            ms.shutdown();
//            Thread.sleep(10000);
//        } catch (Exception e)
//        {
//            logger.error(e.getMessage(), e);
//            System.err.print(e.getMessage());
//        }
//
//    }
//
//    public static void or(String field, String term1, String term2)
//    {
//        try
//        {
//            // step 1,解析配置文件获得配置参数
//            Properties properties = new Properties();
//            properties.load(MSearcher.class.getClassLoader().getResourceAsStream("conf/jessica.properties"));
//
//            Inet4Address inet4Address = NetworkUtil.getIpAddress("eth1");
//            String defaultIp = (inet4Address == null) ? "127.0.0.1" : inet4Address.getHostAddress();
//            String defaultAdminPort = "8723";
//            String defaultUpdatePort = "8724";
//            String defaultSearchPort = "8725";
//            String defaultStoreDir = "/var/data/jessica/store/";
//            String defaultJournalDir = "/var/data/jessica/journal/";
//            String defaultJournalRecover = "false";
//
//            String ip = properties.getProperty("ip", defaultIp);
//            String adminPort = properties.getProperty("admin.port", defaultAdminPort);
//            String updatePort = properties.getProperty("update.port", defaultUpdatePort);
//            String searchPort = properties.getProperty("search.port", defaultSearchPort);
//            String storeDir = properties.getProperty("store.dir", defaultStoreDir);
//            String journalDir = properties.getProperty("journal.dir", defaultJournalDir);
//            String journalRecover = properties.getProperty("journal.recover", defaultJournalRecover);
//
//            final MSearcher ms = MSearcher.getInstance();
//            final MIndexWriter indexWriter = new MIndexWriter();
//            final SimpleBitcaskStore bitcaskStore = new SimpleBitcaskStore(new File(storeDir));
//            final Journal journal = new Journal(new File(journalDir));
//
//            // step 2,创建thrift 网络接口
//            final UpdateServiceImpl updateService = new UpdateServiceImpl(indexWriter, bitcaskStore, journal, ip, Integer.parseInt(updatePort));
//            final SearchServiceImpl searchService = new SearchServiceImpl(indexWriter, bitcaskStore, ip, Integer.parseInt(searchPort));
//            final AdminServiceImpl adminService = new AdminServiceImpl(ms, bitcaskStore, ip, Integer.parseInt(adminPort));
//
//            ms.setIndexWriter(indexWriter);
//            ms.setUpdateService(updateService);
//            ms.setSearchService(searchService);
//            ms.setAdminService(adminService);
//            ms.setBitcaskStore(bitcaskStore);
//            ms.setJournal(journal);
//            ms.setRecover(true);
//
//            // step 4,启动服务器
//            ms.start();
//
//            // step 6,query
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.sortReverse = true;
//            searchRequest.offset = 0;
//            searchRequest.limit = 100000;
//            String _term1 = "{'type':'TermQuery','field':'" + field + "','term':'" + term1 + "'}";
//            String _term2 = "{'type':'TermQuery','field':'" + field + "','term':'" + term2 + "'}";
//            searchRequest.query = "{'type':'OrQuery','queryNodes':[" + _term1 + "," + _term2 + "]}";
//            Map<String, Integer> sortMap = new HashMap<String, Integer>();
//            sortMap.put("timeWeight", 1000);
//            sortMap.put("favWeight", 1000);
//            sortMap.put("editorWeight", 1000);
//            sortMap.put("maxFav", 1000);
//            sortMap.put("maxEditor", 1000);
//            searchRequest.sortMap = sortMap;
//            searchRequest.qtime = 300000;
//            SearchResponse response = searchService.search(searchRequest);
//            List<ResultHit> rs = response.getResults();
//            // step 7,check feed and query
//            logger.error("hits:" + response.getHits());
//
//            // step 8, shutdown
//            ms.shutdown();
//            Thread.sleep(10000);
//        } catch (Exception e)
//        {
//            logger.error(e.getMessage(), e);
//            System.err.print(e.getMessage());
//        }
//
//    }
//
//    public static void and(String field, String term1, String term2)
//    {
//        try
//        {
//            // step 1,解析配置文件获得配置参数
//            Properties properties = new Properties();
//            properties.load(MSearcher.class.getClassLoader().getResourceAsStream("conf/jessica.properties"));
//
//            Inet4Address inet4Address = NetworkUtil.getIpAddress("eth1");
//            String defaultIp = (inet4Address == null) ? "127.0.0.1" : inet4Address.getHostAddress();
//            String defaultAdminPort = "8723";
//            String defaultUpdatePort = "8724";
//            String defaultSearchPort = "8725";
//            String defaultStoreDir = "/var/data/jessica/store/";
//            String defaultJournalDir = "/var/data/jessica/journal/";
//            String defaultJournalRecover = "false";
//
//            String ip = properties.getProperty("ip", defaultIp);
//            String adminPort = properties.getProperty("admin.port", defaultAdminPort);
//            String updatePort = properties.getProperty("update.port", defaultUpdatePort);
//            String searchPort = properties.getProperty("search.port", defaultSearchPort);
//            String storeDir = properties.getProperty("store.dir", defaultStoreDir);
//            String journalDir = properties.getProperty("journal.dir", defaultJournalDir);
//            String journalRecover = properties.getProperty("journal.recover", defaultJournalRecover);
//
//            final MSearcher ms = MSearcher.getInstance();
//            final MIndexWriter indexWriter = new MIndexWriter();
//            final SimpleBitcaskStore bitcaskStore = new SimpleBitcaskStore(new File(storeDir));
//            final Journal journal = new Journal(new File(journalDir));
//
//            // step 2,创建thrift 网络接口
//            final UpdateServiceImpl updateService = new UpdateServiceImpl(indexWriter, bitcaskStore, journal, ip, Integer.parseInt(updatePort));
//            final SearchServiceImpl searchService = new SearchServiceImpl(indexWriter, bitcaskStore, ip, Integer.parseInt(searchPort));
//            final AdminServiceImpl adminService = new AdminServiceImpl(ms, bitcaskStore, ip, Integer.parseInt(adminPort));
//
//            ms.setIndexWriter(indexWriter);
//            ms.setUpdateService(updateService);
//            ms.setSearchService(searchService);
//            ms.setAdminService(adminService);
//            ms.setBitcaskStore(bitcaskStore);
//            ms.setJournal(journal);
//            ms.setRecover(true);
//
//            // step 4,启动服务器
//            ms.start();
//
//            // step 6,query
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.sortReverse = true;
//            searchRequest.offset = 0;
//            searchRequest.limit = 100000;
//            String _term1 = "{'type':'TermQuery','field':'" + field + "','term':'" + term1 + "'}";
//            String _term2 = "{'type':'TermQuery','field':'" + field + "','term':'" + term2 + "'}";
//            searchRequest.query = "{'type':'AndQuery','queryNodes':[" + _term1 + "," + _term2 + "]}";
//            Map<String, Integer> sortMap = new HashMap<String, Integer>();
//            sortMap.put("timeWeight", 1000);
//            sortMap.put("favWeight", 1000);
//            sortMap.put("editorWeight", 1000);
//            sortMap.put("maxFav", 1000);
//            sortMap.put("maxEditor", 1000);
//            searchRequest.sortMap = sortMap;
//            searchRequest.qtime = 300000;
//            SearchResponse response = searchService.search(searchRequest);
//            List<ResultHit> rs = response.getResults();
//            // step 7,check feed and query
//            logger.error("hits:" + response.getHits());
//
//            // step 8, shutdown
//            ms.shutdown();
//            Thread.sleep(10000);
//        } catch (Exception e)
//        {
//            logger.error(e.getMessage(), e);
//            System.err.print(e.getMessage());
//        }
//
//    }
//
//    public static void range(String field, int start, int end)
//    {
//        try
//        {
//            // step 1,解析配置文件获得配置参数
//            Properties properties = new Properties();
//            properties.load(MSearcher.class.getClassLoader().getResourceAsStream("conf/jessica.properties"));
//
//            Inet4Address inet4Address = NetworkUtil.getIpAddress("eth1");
//            String defaultIp = (inet4Address == null) ? "127.0.0.1" : inet4Address.getHostAddress();
//            String defaultAdminPort = "8723";
//            String defaultUpdatePort = "8724";
//            String defaultSearchPort = "8725";
//            String defaultStoreDir = "/var/data/jessica/store/";
//            String defaultJournalDir = "/var/data/jessica/journal/";
//            String defaultJournalRecover = "false";
//
//            String ip = properties.getProperty("ip", defaultIp);
//            String adminPort = properties.getProperty("admin.port", defaultAdminPort);
//            String updatePort = properties.getProperty("update.port", defaultUpdatePort);
//            String searchPort = properties.getProperty("search.port", defaultSearchPort);
//            String storeDir = properties.getProperty("store.dir", defaultStoreDir);
//            String journalDir = properties.getProperty("journal.dir", defaultJournalDir);
//            String journalRecover = properties.getProperty("journal.recover", defaultJournalRecover);
//
//            final MSearcher ms = MSearcher.getInstance();
//            final MIndexWriter indexWriter = new MIndexWriter();
//            final SimpleBitcaskStore bitcaskStore = new SimpleBitcaskStore(new File(storeDir));
//            final Journal journal = new Journal(new File(journalDir));
//
//            // step 2,创建thrift 网络接口
//            final UpdateServiceImpl updateService = new UpdateServiceImpl(indexWriter, bitcaskStore, journal, ip, Integer.parseInt(updatePort));
//            final SearchServiceImpl searchService = new SearchServiceImpl(indexWriter, bitcaskStore, ip, Integer.parseInt(searchPort));
//            final AdminServiceImpl adminService = new AdminServiceImpl(ms, bitcaskStore, ip, Integer.parseInt(adminPort));
//
//            ms.setIndexWriter(indexWriter);
//            ms.setUpdateService(updateService);
//            ms.setSearchService(searchService);
//            ms.setAdminService(adminService);
//            ms.setBitcaskStore(bitcaskStore);
//            ms.setJournal(journal);
//            ms.setRecover(true);
//
//            // step 4,启动服务器
//            ms.start();
//
//            // step 6,query
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.sortReverse = true;
//            searchRequest.offset = 0;
//            searchRequest.limit = 100000;
//            searchRequest.query = "{'type':'RangeQuery','field':'" + field + "','start':'" + start + "','end':'" + end + "'}";
//            Map<String, Integer> sortMap = new HashMap<String, Integer>();
//            sortMap.put("timeWeight", 1000);
//            sortMap.put("favWeight", 1000);
//            sortMap.put("editorWeight", 1000);
//            sortMap.put("maxFav", 1000);
//            sortMap.put("maxEditor", 1000);
//            searchRequest.sortMap = sortMap;
//            searchRequest.qtime = 3;
//            SearchResponse response = searchService.search(searchRequest);
//            List<ResultHit> rs = response.getResults();
//            // step 7,check feed and query
//            logger.error("hits:" + response.getHits());
//
//            // step 8, shutdown
//            ms.shutdown();
//            Thread.sleep(10000);
//        } catch (Exception e)
//        {
//            logger.error(e.getMessage(), e);
//            System.err.print(e.getMessage());
//        }
//
//    }
//
//    public static void e(String field, String term, int docSize)
//    {
//        try
//        {
//            // step 1,解析配置文件获得配置参数
//            Properties properties = new Properties();
//            properties.load(MSearcher.class.getClassLoader().getResourceAsStream("conf/jessica.properties"));
//
//            Inet4Address inet4Address = NetworkUtil.getIpAddress("eth1");
//            String defaultIp = (inet4Address == null) ? "127.0.0.1" : inet4Address.getHostAddress();
//            String defaultAdminPort = "8723";
//            String defaultUpdatePort = "8724";
//            String defaultSearchPort = "8725";
//            String defaultStoreDir = "/var/data/jessica/store/";
//            String defaultJournalDir = "/var/data/jessica/journal/";
//            String defaultJournalRecover = "false";
//
//            String ip = properties.getProperty("ip", defaultIp);
//            String adminPort = properties.getProperty("admin.port", defaultAdminPort);
//            String updatePort = properties.getProperty("update.port", defaultUpdatePort);
//            String searchPort = properties.getProperty("search.port", defaultSearchPort);
//            String storeDir = properties.getProperty("store.dir", defaultStoreDir);
//            String journalDir = properties.getProperty("journal.dir", defaultJournalDir);
//            String journalRecover = properties.getProperty("journal.recover", defaultJournalRecover);
//
//            final MSearcher ms = MSearcher.getInstance();
//            final MIndexWriter indexWriter = new MIndexWriter();
//            final SimpleBitcaskStore bitcaskStore = new SimpleBitcaskStore(new File(storeDir));
//            final Journal journal = new Journal(new File(journalDir));
//
//            // step 2,创建thrift 网络接口
//            final UpdateServiceImpl updateService = new UpdateServiceImpl(indexWriter, bitcaskStore, journal, ip, Integer.parseInt(updatePort));
//            final SearchServiceImpl searchService = new SearchServiceImpl(indexWriter, bitcaskStore, ip, Integer.parseInt(searchPort));
//            final AdminServiceImpl adminService = new AdminServiceImpl(ms, bitcaskStore, ip, Integer.parseInt(adminPort));
//
//            ms.setIndexWriter(indexWriter);
//            ms.setUpdateService(updateService);
//            ms.setSearchService(searchService);
//            ms.setAdminService(adminService);
//            ms.setBitcaskStore(bitcaskStore);
//            ms.setJournal(journal);
//            if (journalRecover.trim().equalsIgnoreCase("true"))
//            {
//                ms.setRecover(true);
//            } else
//            {
//                ms.setRecover(false);
//            }
//
//            // step 4,启动服务器
//            ms.start();
//
//            HashMap<Integer, TDocument> map = new HashMap<Integer, TDocument>();
//            // step 5,feed
//            for (int i = 1; i <= docSize; i++)
//            {
//                TDocument td = new TDocument();
//                TField tfield = new TField();
//                tfield.setName(field);
//                TToken token = new TToken();
//                token.setValue(term);
//
//                tfield.addToTokens(token);
//                td.addToFields(tfield);
//                td.setObject_id(i);
//                Doc doc = new Doc();
//                doc.setUid(i);
//                td.setDoc(doc);
//
//                ArrayList<TDocument> list = new ArrayList<TDocument>();
//                list.add(td);
//                indexWriter.addDocumentDirect(list);
//
//                map.put(i, td);
//            }
//
//            // step 6,query
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.sortReverse = true;
//            searchRequest.offset = 0;
//            searchRequest.limit = 100000;
//            searchRequest.query = "{'type':'TermQuery','field':'" + field + "','term':'" + term + "'}";
//            Map<String, Integer> sortMap = new HashMap<String, Integer>();
//            sortMap.put("timeWeight", 1000);
//            sortMap.put("favWeight", 1000);
//            sortMap.put("editorWeight", 1000);
//            sortMap.put("maxFav", 1000);
//            sortMap.put("maxEditor", 1000);
//            searchRequest.sortMap = sortMap;
//            searchRequest.qtime = 2;
//            SearchResponse response = searchService.search(searchRequest);
//            List<ResultHit> rs = response.getResults();
//            // step 7,check feed and query
//            for (ResultHit resultHit : rs)
//            {
//                map.remove(resultHit.getDocId());
//            }
//
//            System.err.println("map size:" + map.size());
//            // step 8, shutdown
//            ms.shutdown();
//            Thread.sleep(10000);
//        } catch (Exception e)
//        {
//            logger.error(e.getMessage(), e);
//            System.err.print(e.getMessage());
//        }
//    }
//}

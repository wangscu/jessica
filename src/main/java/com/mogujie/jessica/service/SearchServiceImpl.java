package com.mogujie.jessica.service;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.mogujie.jessica.index.IndexWriter;
import com.mogujie.jessica.index.InvertedIndexer;
import com.mogujie.jessica.networking.SimpleServer;
import com.mogujie.jessica.query.Query;
import com.mogujie.jessica.query.QueryNode;
import com.mogujie.jessica.query.SimpleUids;
import com.mogujie.jessica.query.matcher.TermBasedQueryMatcher;
import com.mogujie.jessica.query.matcher.TermMatcher;
import com.mogujie.jessica.scorer.SimpleScorer;
import com.mogujie.jessica.service.thrift.ResultHit;
import com.mogujie.jessica.service.thrift.SearchRequest;
import com.mogujie.jessica.service.thrift.SearchResponse;
import com.mogujie.jessica.service.thrift.SearchService.Iface;
import com.mogujie.jessica.service.thrift.SearchService.Processor;
import com.mogujie.jessica.util.Constants;
import com.mogujie.jessica.util.TimSort;
import com.mogujie.storeage.SimpleBitcaskStore;

/**
 * 搜索接口
 * 
 * @author xuanxi
 * 
 */
public class SearchServiceImpl implements Iface
{
    private static final Logger logger = Logger.getLogger(Constants.LOG_SEARCH);
    private String ip;
    private int port;
    private IndexWriter indexWriter;
    private SimpleServer server;
    private SimpleBitcaskStore bitcaskStore;

    public SearchServiceImpl(IndexWriter indexWriter, SimpleBitcaskStore bitcaskStore, String ip, int port)
    {
        this.indexWriter = indexWriter;
        this.bitcaskStore = bitcaskStore;
        this.ip = ip;
        this.port = port;
    }

    public void start()
    {
        logger.info("启动jessica搜索引擎!");
        try
        {
            Processor<SearchServiceImpl> processor = new Processor<SearchServiceImpl>(this);
            server = new SimpleServer(processor, ip, port);
            server.start();
        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }
    }

    public void shutdown()
    {
        server.shutdonw();
    }

    /**
     * 第2,3,4步骤 可选
     */
    @Override
    public SearchResponse search(SearchRequest searchRequest) throws TException
    {
        try
        {
            long beginTime = System.currentTimeMillis();
            long searchTime = 0;
            long scoreTime = 0;
            long sortTime = 0;
            long dataTime = 0;

            StringBuilder builder = new StringBuilder();
            builder.append(searchRequest.getQuery());

            // step 1, search
            long startSearch = System.currentTimeMillis();
            String json = searchRequest.getQuery();
            // 将query字符串转化为queryNode
            QueryNode root = QueryNode.buildQueryNode(json, true, searchRequest.getQtime());
            InvertedIndexer indexer = indexWriter.getIndexer();
            int[] doc2idArray = indexer.getDocUids();
            TermMatcher termMatcher = new TermMatcher(indexer);
            Query query = new Query(root, "");
            TermBasedQueryMatcher matcher = new TermBasedQueryMatcher(termMatcher, doc2idArray);
            SimpleUids uids = matcher.getAllMatches(query, searchRequest.getLimit() + searchRequest.getOffset());
            searchTime = System.currentTimeMillis() - startSearch;

            // step 2, score
            long startScoreTime = System.currentTimeMillis();
            SimpleScorer scorer = new SimpleScorer(uids.uids(), searchRequest.getSortMap());
            scorer.scoreDocument();
            scoreTime = System.currentTimeMillis() - startScoreTime;

            long[] rs = uids.uids();
            // step 3,sort
            long startSortTime = System.currentTimeMillis();
            TimSort.sort(rs, scorer);
            sortTime = System.currentTimeMillis() - startSortTime;

            // step 4, get store data
            long startDataTime = System.currentTimeMillis();
            SearchResponse response = result(searchRequest, rs);
            dataTime = System.currentTimeMillis() - startDataTime;

            builder.append(", Hits:").append(response.getHits());
            builder.append(", 查询耗时:").append(searchTime).append("ms");
            builder.append(", 打分耗时:").append(scoreTime).append("ms");
            builder.append(", 排序耗时:").append(sortTime).append("ms");
            builder.append(", 组装耗时:").append(dataTime).append("ms");
            builder.append(", 总耗时:").append(System.currentTimeMillis() - beginTime).append("ms!");

            logger.info(builder.toString());

            return response;
        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    private SearchResponse result(SearchRequest searchRequest, long[] rs)
    {

        SearchResponse response = new SearchResponse();
        // 存储符合条件的记录
        int hits = rs.length;
        int offset = searchRequest.getOffset();
        int limit = searchRequest.getLimit();
        List<ResultHit> results = new ArrayList<ResultHit>();
        if (offset > hits)
        {
            // TODO 没有指定偏移量的数据
        } else
        {
            int length = ((offset + limit > hits) ? hits - offset : limit);

            long[] docs = new long[length];
            System.arraycopy(rs, offset, docs, 0, length);
            List<String> fieldsName = searchRequest.getFields();
            for (int i = 0; i < length; i++)
            {
                ResultHit hit = new ResultHit();
                long uid = docs[i];
                Map<String, String> fields = bitcaskStore.get(uid);
                hit.setDocId((int) uid);
                if (fields == null)
                {
                    results.add(hit);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("搜索的时候uid:" + uid + "没有找到指定文档");
                    }
                    continue;
                }

                if (fieldsName == null)
                {
                    hit.setFields(fields);
                } else
                { // 如果有指定 就只返回指定的field
                    Map<String, String> data = new HashMap<String, String>();
                    for (String fieldName : fieldsName)
                    {
                        String fieldValue = fields.get(fieldName);
                        if (fieldValue != null)
                        {
                            data.put(fieldName, fieldValue);
                        }
                    }
                    hit.setFields(data);
                }
                results.add(hit);
            }
        }
        response.setDocs(results.size());
        response.setResults(results);
        response.setHits(hits);
        return response;
    }

    public static class SearchServiceThreadFactory implements ThreadFactory
    {
        public Thread newThread(Runnable runnable)
        {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler()
            {
                @Override
                public void uncaughtException(Thread thread, Throwable e)
                {
                    logger.error("thread:" + thread.getName() + " uncaughtException:" + e.getMessage(), e);
                }
            });
            thread.setName("SearchService-pool-thread-" + threaCounter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        }

        private final static AtomicLong threaCounter = new AtomicLong(0);
    }
}

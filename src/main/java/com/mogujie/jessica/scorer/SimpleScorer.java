/*
 * Copyright (c) 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.mogujie.jessica.scorer;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.mogujie.jessica.query.SimpleUids;
import com.mogujie.jessica.service.thrift.Doc;

/**
 * 按打分的降序排序
 * 
 * @author xuanxi
 * 
 */
public class SimpleScorer implements Comparator<Long>
{
    private final static Logger logger = Logger.getLogger(SimpleScorer.class);
    public final static String CREATED_FIELD = "created";
    public final static String FAV_FIELD = "cfav";
    public final static String FAVCTU_FIELD = "zfavctu";
    public final static String EDITOR_FIELD = "zeditor";
    public final static long NEGATIVE_TO_POSITIVE = 10000;
    private final static Long2ObjectRBTreeMap<String> data = new Long2ObjectRBTreeMap<String>();

    private long[] uids;
    private Map<Long, Long> result;
    private long timeWeight;
    private long favWeight;
    private long editorWeight;
    private long maxFav;
    private long maxEditor;

    public static void updateData(Doc doc)
    {
        long uid = doc.getUid();
        Map<String, String> m = doc.getFields();
        Joiner joiner = Joiner.on("_");
        String d = joiner.join(m.get(CREATED_FIELD), m.get(FAV_FIELD), m.get(FAVCTU_FIELD), m.get(EDITOR_FIELD));
        data.put(uid, d);
    }

    public SimpleScorer(long[] uids, Map<String, Integer> params)
    {
        this.uids = uids;
        this.timeWeight = params.get("timeWeight");
        this.favWeight = params.get("favWeight");
        this.editorWeight = params.get("editorWeight");
        this.maxFav = params.get("maxFav");
        this.maxEditor = params.get("maxEditor");
    }

    public void scoreDocument()
    {
        result = new HashMap<Long, Long>();
        long[] us = uids;
        int size = uids.length;
        for (int i = 0; i < size; i++)
        {
            long uid = us[i];
            String d = data.get(uid);
            Iterator<String> iterator = Splitter.on("_").split(d).iterator();
            long created = Long.parseLong(iterator.next());
            long cfav = Long.parseLong(iterator.next());
            long zfavctu = Long.parseLong(iterator.next());
            long zeditor = Long.parseLong(iterator.next());

            long score = getValue(created, cfav, zfavctu, zeditor);
            result.put(uid, score);
        }

    }

    private long getValue(long created, long cfav, long zfavctu, long zeditor)
    {

        return 0;
    }

    @Override
    public int compare(Long uid1, Long uid2)
    {
        long score1 = result.get(uid1);
        long score2 = result.get(uid2);
        if (logger.isDebugEnabled())
        {
            logger.debug(uid1 + ":" + score1 + "," + uid2 + ":" + score2);
        }
        if (score1 > score2)
        {
            return 1;
        } else if (score1 < score2)
        {
            return -1;
        } else
        {
            return 0;
        }
    }
}

package com.mogujie.jessica.query;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * @author xuanxi
 * 
 */
public abstract class QueryNode implements Serializable
{
    private static final long serialVersionUID = 1L;

    public static QueryNode buildQueryNode(String json, boolean terminalable, int maxMs)
    {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(json);
        return buildQueryNode(element, terminalable, maxMs);
    }

    // TODO 其他几种查询的组装方法的编写
    public static QueryNode buildQueryNode(JsonElement element, boolean terminalable, int maxMs)
    {
        JsonObject object = element.getAsJsonObject();
        String queryNodeType = object.get("type").getAsString();
        object.remove("type");
        QueryNode queryNode = null;
        if (queryNodeType.trim().equals("TermQuery"))
        {
            String field = object.get("field").getAsString();
            String term = object.get("term").getAsString();
            if (terminalable)
            {
                queryNode = new TerminableTermQuery(field, term, maxMs);
            } else
            {
                queryNode = new TermQuery(field, term);
            }
        } else if (queryNodeType.trim().equals("AndQuery"))
        {
            JsonArray queryNodeList = object.get("queryNodes").getAsJsonArray();
            int size = queryNodeList.size();
            QueryNode[] list = new QueryNode[size];
            for (int i = 0; i < size; i++)
            {
                JsonElement jsonElement = queryNodeList.get(i);
                QueryNode node = QueryNode.buildQueryNode(jsonElement, terminalable, maxMs);
                list[i] = node;
            }
            queryNode = new AndQuery(list);
        } else if (queryNodeType.trim().equalsIgnoreCase("OrQuery"))
        {
            JsonArray queryNodeList = object.get("queryNodes").getAsJsonArray();
            int size = queryNodeList.size();
            QueryNode[] list = new QueryNode[size];
            for (int i = 0; i < size; i++)
            {
                JsonElement jsonElement = queryNodeList.get(i);
                QueryNode node = QueryNode.buildQueryNode(jsonElement, terminalable, maxMs);
                list[i] = node;
            }
            queryNode = new OrQuery(list);
        } else if (queryNodeType.trim().equalsIgnoreCase("DifferenceQuery"))
        {
            JsonObject leftNode = object.get("leftNode").getAsJsonObject();
            JsonObject rightNode = object.get("rightNode").getAsJsonObject();
            QueryNode leftQuery = QueryNode.buildQueryNode(leftNode, terminalable, maxMs);
            QueryNode rightQuery = QueryNode.buildQueryNode(rightNode, terminalable, maxMs);
            queryNode = new DifferenceQuery(leftQuery, rightQuery);
        } else if (queryNodeType.trim().equalsIgnoreCase("RangeQuery"))
        {
            String field = object.get("field").getAsString();
            int start = object.get("start").getAsInt();
            int end = object.get("end").getAsInt();
            queryNode = new RangeQuery(field, start, end);
        }

        return queryNode;
    }

    public Set<TermQuery> getPositiveTerms()
    {
        return new HashSet<TermQuery>();
    }

    /*
     * Cloning methods.
     */
    public abstract QueryNode duplicate();

    public Iterable<QueryNode> getChildren()
    {
        return Sets.newHashSet();
    }

}

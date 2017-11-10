package com.ximo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * @description:
 * @author: 朱文赵
 * @date: 2017/11/9
 */
public class Chapter01 {

    /** 一个礼拜的秒速*/
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    /** 每多一票就加432分数*/
    private static final int VOTE_SCORE = 432;
    /** 分页数*/
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        /*选择数据库15*/
        conn.select(15);

        String articleId = postArticle(conn, "朱文赵",
                "redis-in-action", "http://https://github.com/Xikl/redis-in-action");
        System.out.println("添加了一个新的文章， 文章id为：" + articleId);
        System.out.println("这是他的简介：");
        Map<String, String> article = conn.hgetAll("article:" + articleId);
        printArticle(article);

    }

    /**
     * 文章投票
     *
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article) {
        //计算投票截止时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        //获得有序集合中的该值得分数
        if (conn.zscore("time:", article) < cutoff) {
            return;
        }
        String articleId = article.substring(article.indexOf(':') + 1);
        //添加到set中key为voted:articleId, value为 user
        if (conn.sadd("voted:" + articleId, user) == 1) {
            //添加分数 zincrby key score member;
            conn.zincrby("score:", VOTE_SCORE, article);
            //hash结构 该文章投票数加1
            conn.hincrBy(article, "votes", 1);
        }
    }

    /**
     * 发布文章 相当于添加一个文章
     *
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return 文章id
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        //计数器加一 String类型
        String articleId = String.valueOf(conn.incr("article:"));
        String voted = "voted:" + articleId;
        /*添加user到该voted中*/
        conn.sadd(voted, user);
        /*设置过期时间*/
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        /*现在的时间*/
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        Map<String, String> articleData = new HashMap<>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");

        /*存入一个hashMap中*/
        conn.hmset(article, articleData);
        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 获得文章
     *
     * @param conn
     * @param page
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    /**
     * 获得文章集合，将新添加的文章id加入到里面
     *
     * @param conn
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        List<Map<String, String>> articles = new ArrayList<>();
        /*按score递减排列, 获得文章set集合*/
        Set<String> ids = conn.zrevrange(order, start, end);
        /*遍历文章*/
        for (String id : ids) {
            /*获得文章的所有的内容*/
            Map<String, String> articleData = conn.hgetAll(id);
            /*存入该文章的id到该hash中*/
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 添加分组
     *
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        /*key*/
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) {
            ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, zParams, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    /**
     * 打印多个文章 list
     * @param articles
     */
    private void printArticles(List<Map<String, String>> articles) {
        for (Map<String, String> article : articles) {
            System.out.println("id：" + article.get("id"));
            printArticle(article);
        }
    }

    /**
     * 打印单个文章
     * @param article
     */
    private void printArticle(Map<String, String> article){
        for (Map.Entry<String, String> entry : article.entrySet()){
            if("id".equals(entry.getKey())){
                continue;
            }
            System.out.println("     " + entry.getKey() + " : " + entry.getValue());
        }
    }
}

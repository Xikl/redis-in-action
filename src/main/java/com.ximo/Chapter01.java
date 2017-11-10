package com.ximo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: 朱文赵
 * @date: 2017/11/9
 */
public class Chapter01 {

    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {

    }

    public void run(){
        Jedis conn = new Jedis("localhost");

    }

    /**
     * 文章投票
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article){
        //计算投票截止时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        //获得有序集合中的该值得分数
        if(conn.zscore("time:", article) < cutoff){
            return;
        }
        String articleId = article.substring(article.indexOf(':') + 1);
        //添加到set中key为voted:articleId, value为 user
        if(conn.sadd("voted:"+ articleId, user) == 1){
            //添加分数 zincrby key score member;
            conn.zincrby("score:", VOTE_SCORE, article);
            //hash结构 该文章投票数加1
            conn.hincrBy(article, "votes", 1);
        }
    }

    public String postArticle(Jedis conn, String user, String title, String link){
        //计数器加一 String类型
        String articleId = String.valueOf(conn.incr("article:"));
        String voted = "voted:"+articleId;
        conn.sadd(voted, user);
        /*设置过期时间*/
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        /*现在的时间*/
        long now = System.currentTimeMillis() / 1000;
        String article = "article:"+articleId;
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
     * 添加分组
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd){
        String article = "article:"+articleId;
        for (String group : toAdd) {
            conn.sadd("group:"+group, article);
        }
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order){
        String key = order + group;
        if(!conn.exists(key)){
            ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, zParams, "group:"+group, order);
            conn.expire(key, 60);
        }
        //todo
        return null;
    }

}

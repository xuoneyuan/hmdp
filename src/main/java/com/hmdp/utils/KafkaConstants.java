package com.hmdp.utils;

public class KafkaConstants {
    public static final String TOPIC_CREATE_ORDER = "createOrder";
    public static final String TOPIC_LIKE_BEHAVIOR = "like-behavior-topic";
    public static final String TOPIC_SAVE_DB_FAILED = "save-db-failed-topic";
    public static final String TOPIC_LIKE_ARTICLE_COUNT = "like-article-count-topic";
    public static final String TOPIC_LIKE_USER_COUNT = "like-user-count-topic";
    public static final Integer FLUSH_MSG_NUM_LIMIT = 100;
    public static final Integer FLUSH_MSG_INTERVAL = 2000;

}

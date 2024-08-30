package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.hash.Hash;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Event;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.event.KafkaLikeProducer;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.*;
import jdk.jpackage.internal.Log;
import net.bytebuddy.description.field.FieldDescription;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.KafkaConstants.TOPIC_LIKE_BEHAVIOR;
import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private KafkaLikeProducer kafkaLikeProducer;

    @Resource
    private LoadingCache<String,String> cache;

    @Resource
    private BloomFilter bloomFilter;

    @Resource
    private RedisTemplate redisTemplate;

    @PostConstruct
    public LoadingCache<String,String> init(){
        return Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(2, TimeUnit.HOURS)
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String s) throws Exception {
                        return null;
                    }
                });
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        // 1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 2.查询blog有关的用户f
        queryBlogUser(blog);
        // 3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        String key = "blog:liked:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setType(null);
    }



    @Override
    public Result likeBlog(Blog blog) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.判断当前登录用户是否已经点赞
        Long blogId = blog.getBlogId();
        Integer type = blog.getType();
        String key = BLOG_LIKED_KEY + blogId;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 3.如果未点赞，可以点赞
            // 3.1.数据库点赞数 + 1
            boolean isSuccess = update()
                    .setSql("likeCount = likeCount + 1")
                    .eq("blogId", blogId)
                    .update();
            // 3.2.保存用户到Redis的set集合  zadd key value score
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4.如果已点赞，取消点赞
            // 4.1.数据库点赞数 -1
            boolean isSuccess = update()
                    .setSql("likeCount = likeCount  - 1")
                    .eq("blogId", blogId)
                    .update();
            // 4.2.把用户从Redis的set集合移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        int diff = type == 1?1:-1;

        updateLocalCache(blogId,diff);
        updateRedis(blogId,userId,diff);

        Long likedId = redisIdWorker.nextId("like_behavior");

        sendLikeBehaviorMsg(likedId,blogId,userId,type);

        return Result.ok();
    }

    private void updateRedis(Long blogId,Long userId,int diff){
        updateRedisZset(blogId, userId, diff);
        //  更新文章的点赞计数
        String articleLikeCountKey = Blog_LIKE_COUNT + blogId;
        String count = stringRedisTemplate.opsForValue().get(articleLikeCountKey);

        if (count == null) {
            // 如果 Redis 中没有相关数据
            if (diff == 1) {
                // 并且本次是点赞操作，先暂时把它存为 1
                redisTemplate.opsForValue().set(articleLikeCountKey, "1");
            }
            // 如果本次是取消赞操作，先不做任何处理
        } else {
            // 如果 Redis 中有相关数据，直接更新值
            int newCount = Integer.parseInt(count) + diff;
            if (newCount < 0) {
                log.error("文章获赞总数数据异常，准备删除异常的 Redis 数据...");
                redisTemplate.delete(articleLikeCountKey);
                log.error("异常数据删除成功！");
            } else {
                redisTemplate.opsForValue().set(articleLikeCountKey, String.valueOf(newCount));
            }
        }
    }

    private void updateRedisZset(Long blogId, Long userId, int diff) {
        if(diff==1) {
            double score = Instant.now().getEpochSecond();
            if (!redisTemplate.opsForZSet().add(BLOG_LIKED_KEY + blogId, userId, score)){
                log.error("添加点赞到zset失败");
                redisTemplate.delete(BLOG_LIKED_KEY+blogId);
                return;
            }
        }
    }

    private void updateLocalCache(Long blogId,int diff){
        Integer blogCount = Integer.valueOf(cache.get(BLOG_LIKED_KEY + blogId));
        if(blogCount!=null)
        {
            cache.put(BLOG_LIKED_KEY+blogId,String.valueOf(blogCount+diff));
        }

    }

    private void sendLikeBehaviorMsg(Long likedId,Long blogId,Long userId,Integer type){
        HashMap<String, Object> data = new HashMap<>();
        data.put("likedId",likedId);
        data.put("blogId",blogId);
        data.put("userId",userId);
        data.put("type",type);
        Event event = new Event()
                .setTopic(TOPIC_LIKE_BEHAVIOR)
                .setUserId(userId)
                .setData(data);
        kafkaLikeProducer.publishEvent(event);
    }

    private boolean isLike(Long blogId, Long userId){
        if(!bloomFilter.isExist(BLOOM_FILTER,blogId.toString(),userId.toString())){
            return false;
        }
        if ((redisTemplate.opsForZSet().score(USER_LIKE_KEY + userId, blogId.toString()) != null)
                || (redisTemplate.opsForZSet().score(USER_LIKE_KEY + blogId, userId.toString()) != null)) {
            return true;
        }
        Blog one = this.getOne(new LambdaQueryWrapper<Blog>()
                .eq(Blog::getBlogId,blogId)
                .eq(Blog::getUserId,userId)
                .orderByDesc(Blog::getCreateTime)
                .last("LIMIT 1"));

        if(one!=null&&one.getType()==1){
            return true;
        }
        return false;
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店笔记
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增笔记失败!");
        }
        // 3.查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        // 4.推送笔记id给所有粉丝
        for (Follow follow : follows) {
            // 4.1.获取粉丝id
            Long userId = follow.getUserId();
            // 4.2.推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 5.返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 3.非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 4.解析数据：blogId、minTime（时间戳）、offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0; // 2
        int os = 1; // 2
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) { // 5 4 4 2 2
            // 4.1.获取id
            ids.add(Long.valueOf(tuple.getValue()));
            // 4.2.获取分数(时间戳）
            long time = tuple.getScore().longValue();
            if(time == minTime){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }

        // 5.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
            // 5.1.查询blog有关的用户
            queryBlogUser(blog);
            // 5.2.查询blog是否被点赞
            isBlogLiked(blog);
        }

        // 6.封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);

        return Result.ok(r);
    }

    @Override
    @Async
    public CompletableFuture<Void> updateBatchCount(List<Blog> likeBlogCountList) {
        for (Blog likeBlogCount : likeBlogCountList) {
            LambdaQueryWrapper<Blog> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(Blog::getBlogId, likeBlogCount.getBlogId());
            Blog one = this.getOne(wrapper);

            if(one!=null){
                one.setLiked(one.getLiked()+likeBlogCount.getLiked());
                this.updateById(one);
            }else{
                this.save(likeBlogCount);
            }

        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Map<Long, Integer> queryBatchCount(List<Long> blogIds) {
        HashMap<Long, Integer> counts = new HashMap<>();
        List<Long> queryIds = new ArrayList<>();
        for (Long blogId : blogIds) {

            String count = cache.get("blog_like_count:"+blogId);
            if(count!=null){
                counts.put(blogId, Integer.valueOf(count));
            }else{
                queryIds.add(blogId);
            }
        }
        counts.putAll(this.queryBatchCount(queryIds));

        return counts;
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

}

package com.hmdp.utils;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.entity.Blog;
import com.hmdp.mapper.BlogMapper;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Component
public class BloomFilter {

    @Resource
    private BlogMapper blogMapper;

    @Resource
    private RedissonClient redissonClient;

    private static final int PAGE_SIZE = 1000;
    private static final String LIKE_BEHAVIOR_BLOOM_FILTER = "like-behavior-bloom-filter";

    public void initBloomFilter(){
        RBloomFilter<Object> bloomFilter = redissonClient.getBloomFilter(LIKE_BEHAVIOR_BLOOM_FILTER);
        bloomFilter.tryInit(1000000L,0.01);

        int pageNum = 1;
        List<String> keys = getKeysFromDatabase(pageNum, PAGE_SIZE);

        while (!keys.isEmpty()) {
            for (String key : keys) {
                addKeyToBloomFilter(key);
                bloomFilter.add(key);
            }
            pageNum++;
            keys = getKeysFromDatabase(pageNum, PAGE_SIZE);
        }
    }
    private List<String> getKeysFromDatabase(int pageNum,int pageSize){
        Page<Blog> page = new Page<>(pageNum,pageSize);
        QueryWrapper<Blog> blogQueryWrapper = new QueryWrapper<>();
        blogQueryWrapper.like("type",1);

        Page<Blog> blogPage = blogMapper.selectPage(page, blogQueryWrapper);

        List<Blog> records = blogPage.getRecords();
        List<String> keys = new ArrayList<>();

        for (Blog record : records) {
            if(record.getType()==1){
                keys.add(record.getBlogId().toString()+record.getUserId().toString());

            }
        }
        return keys;

    }

    private boolean addKeyToBloomFilter(String name, String ... args){
        RBloomFilter<Object> bloomFilter = redissonClient.getBloomFilter(name);
        StringBuilder key = new StringBuilder();
        for (String arg : args) {
            key.append(arg);
        }
        return bloomFilter.add(key.toString());
    }


    public boolean isExist(String name, String ... args){
        RBloomFilter<Object> bloomFilter = redissonClient.getBloomFilter(name);
        StringBuilder key = new StringBuilder();
        for (String arg : args) {
            key.append(arg);
        }
        return bloomFilter.contains(key.toString());
    }

}

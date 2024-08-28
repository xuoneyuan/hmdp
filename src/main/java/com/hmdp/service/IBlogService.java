package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.entity.Blog;
import com.hmdp.dto.Result;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {
    Result queryHotBlog(Integer current);

    Result queryBlogById(Long id);

    Result likeBlog(Blog blog);



    Result saveBlog(Blog blog);

    Result queryBlogOfFollow(Long max, Integer offset);


    CompletableFuture<Void> updateBatchCount(List<Blog> likeBlogCountList);

    Map<Long, Integer> queryBatchCount(List<Long> blogIds);
}

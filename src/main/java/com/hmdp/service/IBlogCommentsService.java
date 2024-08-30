package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.BlogComments;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogCommentsService extends IService<BlogComments> {


    /**
     * 添加评论
     * @param blogComments
     * @return
     */
    Result addComment(BlogComments blogComments);

    Result queryCommentsByBlog(Long blogId,Integer current);
}



package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.BlogComments;
import com.hmdp.mapper.BlogCommentsMapper;
import com.hmdp.service.IBlogCommentsService;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogCommentsServiceImpl extends ServiceImpl<BlogCommentsMapper, BlogComments> implements IBlogCommentsService {

    @Override
    public Result addComment(BlogComments blogComments) {
        Long userId = UserHolder.getUser().getId();
        blogComments.setUserId(userId);

        boolean success = save(blogComments);
        if(success){
            return Result.ok();
        }else{
            return Result.fail("添加评论失败");
        }

    }

    @Override
    public Result queryCommentsByBlog(Long blogId, Integer current) {
        Page<BlogComments> page = query()
                .eq("blogId",blogId)
                .orderByDesc("create_time")
                .page(new Page<>(current, 10));

        return Result.ok(page.getRecords());
    }

}

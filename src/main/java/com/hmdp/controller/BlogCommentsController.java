package com.hmdp.controller;

import com.hmdp.entity.BlogComments;
import com.hmdp.dto.Result;
import org.springframework.web.bind.annotation.*;
import com.hmdp.service.IBlogCommentsService;
import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/blog-comments")
public class BlogCommentsController {

    @Resource
    private IBlogCommentsService blogCommentsService;

    @PostMapping
    public Result addComment(@RequestBody BlogComments blogComments){
        return blogCommentsService.addComment(blogComments);
    }

    @GetMapping("/query/comment/{blogId}")
    public Result queryCommentsByBlog(@PathVariable("blogId") Long blogId,
                                      @RequestParam(value = "current",defaultValue = "1")Integer current){
        return blogCommentsService.queryCommentsByBlog(blogId,current);
    }


}

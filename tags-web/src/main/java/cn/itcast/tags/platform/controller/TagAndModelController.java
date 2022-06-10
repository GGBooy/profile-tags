package cn.itcast.tags.platform.controller;

import cn.itcast.tags.platform.entity.Codes;
import cn.itcast.tags.platform.entity.HttpResult;
import cn.itcast.tags.platform.entity.dto.ModelDto;
import cn.itcast.tags.platform.entity.dto.TagDto;
import cn.itcast.tags.platform.entity.dto.TagModelDto;
import cn.itcast.tags.platform.service.TagAndModelService;
import cn.itcast.tags.up.HDFSUtils;
import cn.itcast.tags.utils.SearchUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ClassUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
public class TagAndModelController {

    @Autowired
    private TagAndModelService service;

    /**
     * 添加标签
     */
    @PutMapping("tags/relation")
    public void putTags(@RequestBody List<TagDto> tags) {
        service.addTagsByRelation(tags);
        System.out.println("success..");
    }


    @GetMapping("tags")
    public HttpResult<List<TagDto>> getTagByLevelOrId(@RequestParam(required = false) Integer level,
                                                      @RequestParam(required = false) Long pid) {

        List<TagDto> list = null;

        if (level == null && pid != null) {
            //根据ID查找
            list = service.findByPid(pid);
        }
        if (level != null && pid == null) {
            //根据等级查找查找
            list = service.findByLevel(level);
        }

        return new HttpResult<List<TagDto>>(Codes.SUCCESS, "查询成功", list);
    }

    /**
     * 四级界面新增标签模型
     *
     * @param tagModelDto
     * @return
     */
    @PutMapping("tags/model1")
    public HttpResult putModel(@RequestBody TagModelDto tagModelDto) {
        System.out.println("tagModelDto1:" + tagModelDto);
        service.addTagModel(tagModelDto.getTag(), tagModelDto.getModel());
        return new HttpResult(Codes.SUCCESS, "成功", null);
    }

    /**
     * 基于 pid task or tag
     * 如果pid是三级标签 显示task
     * 如果pid是四级标签 显示tag
     *
     * @param pid
     * @return
     */
    @GetMapping("tags/model2")
    public HttpResult getModel(Long pid) {
        System.out.println("model2: pid = " + pid);

        List<TagModelDto> dto = service.findModelByPid(pid);
        return new HttpResult(Codes.SUCCESS, "查询成功", dto);
    }

    @PutMapping("tags/data")
    /**
     * 添加五级标签
     */
    public HttpResult putData(@RequestBody TagDto tagDto) {
        service.addDataTag(tagDto);
        return new HttpResult(Codes.SUCCESS, "添加成功", null);
    }

    @PostMapping("tags/{id}/model")
    public HttpResult changeModelState(@PathVariable Long id, @RequestBody ModelDto modelDto) {
        System.out.println(id + "==>>" + modelDto.getState());
        service.updateModelState(id, modelDto.getState());
        return new HttpResult(Codes.SUCCESS, "执行成功", null);
    }


    @PostMapping("/tags/upload")
    public HttpResult uploadFile(MultipartFile file) {
        String path = HDFSUtils.getInstance().getDefaultFs() + "/apps/temp/jars/" + UUID.randomUUID().toString() + ".jar";
        String contentType = file.getContentType();
        System.out.println(contentType);
        long size = file.getSize();

        System.out.println(size);
        try {
            HDFSUtils.getInstance().copyFromInput(file.getInputStream(), path);
        } catch (IOException e) {
            e.printStackTrace();
            return new HttpResult(Codes.ERROR_UPLOAD, "上传失败", null);
        }
        return new HttpResult(Codes.SUCCESS, "上传成功", path);
    }


    @GetMapping("tags/search")
    public HttpResult getFamilyName(@RequestParam String tableName,
                                    @RequestParam String columnName,
                                    @RequestParam(required = false) Integer method,
                                    @RequestParam String matchValue) {
        String path = SearchUtils.search(tableName, columnName, method, matchValue);
        HDFSUtils instance = HDFSUtils.getInstance();
        URI uri = instance.uri;
        System.out.println(uri);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(uri, instance.config, "gjl");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Path s_path = new Path(path);

        try {
            for (FileStatus status : fs.listStatus(s_path)) {
                String filepath = status.getPath().toString();
                if (filepath.endsWith("csv")) {
                    String realPath = ClassUtils.getDefaultClassLoader().getResource("").getPath() + "static/";
                    instance.copyToLocal(filepath, realPath + "searchResult.csv");
                    System.out.println("复制成功");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HttpResult(Codes.SUCCESS, "获取成功", "searchResult.csv");
    }

}

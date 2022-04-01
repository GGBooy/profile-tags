package cn.itcast.tags.platform.service;


import cn.itcast.tags.platform.entity.dto.ModelDto;

public interface Engine {

    String startModel(ModelDto modelDto);
    void stopModel(ModelDto modelDto);
}

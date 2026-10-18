package com.example.remotedatasync.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.remotedatasync.po.SyncWatermark;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 同步水位表 Mapper。
 *
 * @author 50707
 */
public interface SyncWatermarkMapper extends BaseMapper<SyncWatermark> {

    /** 全量加载（崩溃恢复时装载全部已确认水位） */
    @Select("SELECT id, source_ip, source_db, table_name, watermark_time, watermark_key, update_time "
            + "FROM sync_watermark")
    List<SyncWatermark> selectAll();

    /** 批量 upsert：按唯一键 (source_ip, source_db, table_name) 幂等更新 */
    @Insert("<script>"
            + "INSERT INTO sync_watermark (source_ip, source_db, table_name, watermark_time, watermark_key, update_time) VALUES "
            + "<foreach collection='list' item='w' separator=','>"
            + "(#{w.sourceIp}, #{w.sourceDb}, #{w.tableName}, #{w.watermarkTime}, #{w.watermarkKey}, NOW())"
            + "</foreach>"
            + " ON DUPLICATE KEY UPDATE watermark_time = VALUES(watermark_time), "
            + "watermark_key = VALUES(watermark_key), update_time = NOW()"
            + "</script>")
    void upsertBatch(@Param("list") List<SyncWatermark> list);

    /** 删除单表水位（DDL 变更 / 主备切换前，使「需全量重扫」立即落盘） */
    @Delete("DELETE FROM sync_watermark WHERE source_ip = #{sourceIp} "
            + "AND source_db = #{sourceDb} AND table_name = #{tableName}")
    int deleteByKey(@Param("sourceIp") String sourceIp, @Param("sourceDb") String sourceDb,
                    @Param("tableName") String tableName);

    /** 删除某源下全部水位（主备切换后强制全量重扫） */
    @Delete("DELETE FROM sync_watermark WHERE source_ip = #{sourceIp}")
    int deleteBySourceIp(@Param("sourceIp") String sourceIp);
}

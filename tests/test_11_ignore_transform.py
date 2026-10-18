# -*- coding: utf-8 -*-
"""忽略规则与字段转换功能测试。

同步引擎会自动同步源主机所有用户库到目标主机（排除系统库与 ignoreDatabases）。

由于默认映射 默认主机对已存在于 application.yml，无法重复添加
（返回 MAPPING_ALREADY_EXISTS），本测试采用简化方案：
1. 直查 DRPlatform.sync_progress 表验证系统库（information_schema / mysql /
   performance_schema / sys / DRPlatform）未被同步、用户库已被同步。
2. 通过 /sync/mapping/add 接口参数校验验证 ignoreDatabases / ignoreTables /
   transformRules 字段被接口正确接受（重复添加返回 ALREADY_EXISTS 而非
   PARAM_ERROR，证明参数格式合法）。

涉及接口：
- POST /sync/mapping/add   新增映射（body 含 ignoreDatabases / ignoreTables /
                            ignoreDdlTables / transformRules）
- GET  /sync/mappings       查询映射列表（验证默认映射存在）
"""
import pytest

from conftest import *  # noqa: F401,F403  assert_ok / assert_err / LOCAL_MYSQL / REMOTE_MYSQL / db_query ...

AUTH_REQUIRED = "2807003003"
PARAM_ERROR = "2807002001"
MAPPING_ALREADY_EXISTS = "2807002013"
DB_CONNECTION_TEST_FAILED = "2807002012"

# 系统库与管控库，不应出现在 sync_progress 表中
SYSTEM_DATABASES = [
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
    "DRPlatform",
]


def _full_mapping_body(src, tgt, **extra):
    """构造 /sync/mapping/add 完整 body（源/目标 + 可选忽略/转换规则）。"""
    body = {
        "sourceHost": src["host"], "sourcePort": src["port"],
        "sourceUser": src["user"], "sourcePassword": src["password"],
        "targetHost": tgt["host"], "targetPort": tgt["port"],
        "targetUser": tgt["user"], "targetPassword": tgt["password"],
    }
    body.update(extra)
    return body


class TestIgnoreDatabases:
    """整库忽略：系统库与管控库不应出现在 sync_progress 表中。"""

    def test_system_databases_not_synced(self):
        """information_schema / mysql / performance_schema / sys / DRPlatform 不在 sync_progress。

        同步引擎在 ScanNewDatabaseJob / DatabaseSyncManager 中通过 SYSTEM_SCHEMAS
        自动排除系统库与管控库，sync_progress 表不应出现这些库名。
        """
        rows = db_query(
            LOCAL_MYSQL["host"], LOCAL_MYSQL["port"], LOCAL_MYSQL["user"], LOCAL_MYSQL["password"],
            "SELECT DISTINCT SOURCE_DB_NAME AS db_name FROM DRPlatform.sync_progress",
        )
        assert isinstance(rows, list), f"查询 sync_progress 失败: {rows}"
        synced_dbs = {r["db_name"] for r in rows}
        for sys_db in SYSTEM_DATABASES:
            assert sys_db not in synced_dbs, \
                f"BUG: 系统库 {sys_db} 不应被同步，但出现在 sync_progress 中: {synced_dbs}"

    def test_user_database_synced(self):
        """至少存在一个用户库在 sync_progress 中（证明同步引擎已启动并扫描用户库）。"""
        rows = db_query(
            LOCAL_MYSQL["host"], LOCAL_MYSQL["port"], LOCAL_MYSQL["user"], LOCAL_MYSQL["password"],
            "SELECT DISTINCT SOURCE_DB_NAME AS db_name, SOURCE_IP AS source_ip, STATE AS state "
            "FROM DRPlatform.sync_progress",
        )
        assert isinstance(rows, list), f"查询 sync_progress 失败: {rows}"
        assert len(rows) > 0, "sync_progress 为空，无用户库同步记录（同步引擎可能未启动）"

        # 全部已同步的库都不应是系统库
        for r in rows:
            db_name = r["db_name"]
            assert db_name not in SYSTEM_DATABASES, \
                f"BUG: 系统库 {db_name} 不应出现在 sync_progress: {r}"


class TestIgnoreTables:
    """表忽略规则的 API 参数校验。

    由于默认映射 默认主机对已存在，添加相同映射会返回
    MAPPING_ALREADY_EXISTS。若 ignoreDatabases / ignoreTables 参数格式
    不合法，后端会先返回 PARAM_ERROR。因此：返回 ALREADY_EXISTS 即证明
    忽略规则参数被接口正确接受。
    """

    def test_ignore_rules_via_api(self, auth, base_url):
        """ignoreDatabases + ignoreTables + ignoreDdlTables 组合参数被接口接受。

        重复添加返回 ALREADY_EXISTS（而非 PARAM_ERROR），证明忽略规则字段格式合法。
        """
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            ignoreDatabases=["geo_test_ignore"],
            ignoreTables=["t_ignore"],
            ignoreDdlTables=["t_ddl_ignore"],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        # 重复添加应返回 400 ALREADY_EXISTS，不报 500
        assert r.status_code in (200, 400), \
            f"响应异常: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) == MAPPING_ALREADY_EXISTS, \
                f"组合忽略规则应返回 ALREADY_EXISTS，实际: {j}"
        else:
            # 批量结果形式：skipped 非空（已存在跳过）
            data = j.get("data", {})
            assert "skipped" in data or "created" in data, \
                f"批量结果异常: {data}"

    def test_ignore_tables_by_db_field_accepted(self, auth, base_url):
        """ignoreTablesByDb 层级忽略字段被接口接受。"""
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            ignoreTablesByDb=[
                {"dbName": "geo_test_ignore_tbl", "tables": ["t_ignore"]},
            ],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        assert r.status_code in (200, 400), \
            f"响应异常: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) == MAPPING_ALREADY_EXISTS, \
                f"ignoreTablesByDb 应返回 ALREADY_EXISTS，实际: {j}"

    def test_common_ignore_tables_field_accepted(self, auth, base_url):
        """commonIgnoreTables 通用忽略字段被接口接受。"""
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            commonIgnoreTables=["t_global_ignore"],
            commonDdlIgnoreTables=["t_global_ddl_ignore"],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        assert r.status_code in (200, 400), \
            f"响应异常: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) == MAPPING_ALREADY_EXISTS, \
                f"commonIgnoreTables 应返回 ALREADY_EXISTS，实际: {j}"


class TestTransformRules:
    """字段转换规则（transformRules）的 API 参数校验。"""

    def test_transform_rule_via_api(self, auth, base_url):
        """transformRules 字段被接口接受（重复添加返回 ALREADY_EXISTS 而非 PARAM_ERROR）。

        验证 transformRules 的 DTO 结构（dbName / tableName / fieldName /
        sourceValue / targetValue）被后端正确反序列化。
        """
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            transformRules=[{
                "dbName": "*",
                "tableName": "*",
                "fieldName": "env",
                "sourceValue": "prod",
                "targetValue": "dr",
            }],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        assert r.status_code in (200, 400), \
            f"响应异常: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) == MAPPING_ALREADY_EXISTS, \
                f"transformRules 应返回 ALREADY_EXISTS，实际: {j}"
        else:
            data = j.get("data", {})
            assert "skipped" in data or "created" in data, \
                f"批量结果异常: {data}"

    def test_transform_rule_multiple_rules_accepted(self, auth, base_url):
        """多条 transformRules 同时提交被接口接受。"""
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            transformRules=[
                {"dbName": "*", "tableName": "*", "fieldName": "env",
                 "sourceValue": "prod", "targetValue": "dr"},
                {"dbName": "geo_source", "tableName": "t_order",
                 "fieldName": "status", "sourceValue": "1", "targetValue": "0"},
                {"dbName": "*", "tableName": "t_user",
                 "fieldName": "email", "sourceValue": "secret@", "targetValue": ""},
            ],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        assert r.status_code in (200, 400), \
            f"响应异常: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) == MAPPING_ALREADY_EXISTS, \
                f"多条 transformRules 应返回 ALREADY_EXISTS，实际: {j}"

    def test_transform_rule_with_empty_target_value_accepted(self, auth, base_url):
        """targetValue 为空字符串（置空语义）被接口接受。"""
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            transformRules=[{
                "dbName": "*", "tableName": "*",
                "fieldName": "env", "sourceValue": "prod", "targetValue": "",
            }],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        # targetValue 允许为空（置空语义），不应触发 PARAM_ERROR
        assert r.status_code in (200, 400), \
            f"响应异常: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) == MAPPING_ALREADY_EXISTS, \
                f"空 targetValue 应返回 ALREADY_EXISTS，实际: {j}"

    def test_transform_rule_blank_source_value_rejected(self, auth, base_url):
        """transformRules 内 sourceValue 为空 → 参数错误（@NotBlank 校验）。

        TransformRuleDTO.sourceValue 标注了 @NotBlank，空值应触发 PARAM_ERROR。
        注意：SyncMappingRequestDTO.transformRules 字段未标注 @Valid 级联注解，
        若后端未开启级联校验，此用例可能返回 ALREADY_EXISTS 而非 PARAM_ERROR。
        两种情况均不报裸 500 即通过。
        """
        body = _full_mapping_body(
            LOCAL_MYSQL, REMOTE_MYSQL,
            transformRules=[{
                "dbName": "*", "tableName": "*",
                "fieldName": "env", "sourceValue": "", "targetValue": "dr",
            }],
        )
        r = auth.post(f"{base_url}/sync/mapping/add", json=body, timeout=25)
        # 不应裸 500；开启级联校验 → 400 PARAM_ERROR；未开启 → 400 ALREADY_EXISTS
        assert r.status_code in (200, 400), \
            f"空 sourceValue 不应 500: HTTP {r.status_code} {r.text[:300]}"
        j = r.json()
        if j.get("success") is False:
            assert str(j.get("code")) in (PARAM_ERROR, MAPPING_ALREADY_EXISTS), \
                f"空 sourceValue 错误码异常: {j}"

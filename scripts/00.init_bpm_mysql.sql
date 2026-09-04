-- ============================================================
-- GFlow 工作流表建表脚本（MySQL 8.0.13+）
--
-- 建表清单：wf_process / wf_instance / wf_hi_instance /
--           wf_task / wf_task_assignee / wf_hi_task / wf_task_comment
-- 仅包含工作流引擎自身的表；用户/角色/部门等系统表由宿主应用负责。
--
-- 用法：
--   mysql -u root -p gflow < scripts/00.init_bpm_mysql.sql
--   （或 bash scripts/init-db.sh）
--
-- 说明：
--   - 脚本幂等（CREATE TABLE IF NOT EXISTS，索引内联）：重复执行只建缺失
--     的表，不会改动已有表和数据，也不会更新已有表结构。需要重置时请
--     重建数据库；已有实例的结构升级由宿主的迁移机制负责。
--   - 字段约定：主键 VARCHAR(36)；引用列（*_id/*_by，含 tenant_id）统一
--     VARCHAR(64)；时间列 DATETIME(3)（PG 版为 TIMESTAMPTZ），由应用层
--     维护写入；排序规则 utf8mb4_unicode_ci。
--   - 修改表结构时：同步更新两个方言脚本与宿主迁移记录（gflow 仓库
--     internal/migrations），并重新同步 gflow 仓库的 scripts/engine/ 快照。
-- ============================================================

-- 1. 流程定义表（同一租户内 process_key 按 version 递增，保留多个发布版本）
CREATE TABLE IF NOT EXISTS wf_process (
    id VARCHAR(36) PRIMARY KEY COMMENT '主键UUID',
    process_key VARCHAR(100) NOT NULL COMMENT '流程键（业务唯一标识）',
    name VARCHAR(200) NOT NULL COMMENT '流程名称',
    version INT NOT NULL COMMENT '版本号（从 1 开始递增）',
    category VARCHAR(100) COMMENT '分类（可用于权限/报表筛选）',
    description VARCHAR(500) COMMENT '流程描述',
    definition_json JSON NOT NULL COMMENT '流程定义DSL',
    status VARCHAR(20) NOT NULL DEFAULT 'active' COMMENT '状态：active=生效，retired=已停用',
    publish_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '版本发布时间',
    icon VARCHAR(200) NOT NULL DEFAULT '' COMMENT '流程图标',
    process_type VARCHAR(20) NOT NULL DEFAULT 'main' COMMENT '流程定义类型：main 主流程、sub 子流程',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    created_by VARCHAR(64) NOT NULL DEFAULT '' COMMENT '创建人',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    updated_by VARCHAR(64) COMMENT '更新人',
    updated_at DATETIME(3) NULL DEFAULT NULL COMMENT '更新时间',
    ext JSON DEFAULT ('{}') COMMENT '结构化扩展字段',
    UNIQUE KEY uq_process_key_version (tenant_id, process_key, version),
    KEY idx_process_category (category),
    KEY idx_process_status (status),
    KEY idx_process_publish (publish_time DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='流程定义主表（唯一约束带 tenant_id：多租户可使用相同 process_key）';

-- 2. 运行时流程实例表
CREATE TABLE IF NOT EXISTS wf_instance (
    id VARCHAR(36) PRIMARY KEY COMMENT '主键UUID',
    process_id VARCHAR(64) NOT NULL COMMENT '流程定义ID',
    business_key VARCHAR(200) COMMENT '业务键,业务系统唯一编号',
    name VARCHAR(200) NOT NULL COMMENT '实例名称',
    start_user_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '流程发起人用户ID；系统触发时为空字符串',
    status VARCHAR(20) NOT NULL DEFAULT 'active' COMMENT '生命周期状态：draft / active / completed / suspended / terminated / cancelled / failed',
    variables TEXT DEFAULT ('{}') COMMENT '流程变量',
    current_activity VARCHAR(100) COMMENT '当前运行到的节点定义ID',
    priority INT NOT NULL DEFAULT 50 COMMENT '优先级（数值越大越优先）',
    parent_id VARCHAR(64) COMMENT '父流程实例ID（用于子流程）',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    created_by VARCHAR(64) NOT NULL DEFAULT '' COMMENT '发起人用户ID',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '实例创建时间',
    updated_by VARCHAR(64) COMMENT '最后更新人',
    updated_at DATETIME(3) NULL DEFAULT NULL COMMENT '最后更新时间',
    end_reason VARCHAR(2000) COMMENT '完成/终止原因（包含错误信息）',
    duration BIGINT COMMENT '运行时长（毫秒）',
    ended_at DATETIME(3) COMMENT '结束时间',
    KEY idx_instance_status (status),
    KEY idx_instance_tenant (tenant_id),
    KEY idx_instance_biz_key (business_key),
    KEY idx_instance_parent (parent_id),
    KEY idx_instance_created_at (created_at DESC),
    KEY idx_instance_priority (priority DESC, created_at ASC),
    KEY idx_instance_tenant_status (tenant_id, status, created_at DESC),
    KEY idx_instance_tenant_starter (tenant_id, start_user_id),
    KEY idx_instance_process (process_id, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='运行时流程实例表';

-- 3. 历史流程实例表
CREATE TABLE IF NOT EXISTS wf_hi_instance (
    id VARCHAR(36) PRIMARY KEY COMMENT '主键UUID',
    process_id VARCHAR(64) NOT NULL COMMENT '流程定义ID',
    business_key VARCHAR(200) COMMENT '业务键',
    name VARCHAR(200) NOT NULL COMMENT '实例名称',
    start_user_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '流程发起人用户ID；系统触发时为空字符串',
    status VARCHAR(20) NOT NULL COMMENT '生命周期状态：draft / active / completed / suspended / terminated / cancelled / failed',
    variables TEXT DEFAULT ('{}') COMMENT '流程变量',
    current_activity VARCHAR(100) COMMENT '最后停留节点',
    priority INT NOT NULL DEFAULT 50 COMMENT '优先级',
    parent_id VARCHAR(64) COMMENT '父流程实例ID',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    created_by VARCHAR(64) NOT NULL COMMENT '发起人',
    created_at DATETIME(3) NOT NULL COMMENT '实例创建时间',
    updated_by VARCHAR(64) COMMENT '最后更新人',
    updated_at DATETIME(3) COMMENT '最后更新时间',
    end_reason VARCHAR(2000) COMMENT '完成/终止原因（包含错误信息）',
    duration BIGINT COMMENT '运行时长（毫秒）',
    ended_at DATETIME(3) COMMENT '流程结束时间',
    KEY idx_hist_instance_tenant (tenant_id),
    KEY idx_hist_instance_status (status),
    KEY idx_hist_instance_biz_key (business_key),
    KEY idx_hist_instance_parent (parent_id),
    KEY idx_hist_instance_created (created_at DESC),
    KEY idx_hist_instance_ended (ended_at DESC),
    KEY idx_hist_instance_duration (duration),
    KEY idx_hist_inst_tenant_status (tenant_id, status, created_at DESC),
    KEY idx_hist_inst_tenant_starter (tenant_id, start_user_id),
    KEY idx_hist_instance_process (process_id, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='历史流程实例表';

-- 4. 运行时任务表（仅进行中任务）
CREATE TABLE IF NOT EXISTS wf_task (
    id VARCHAR(36) PRIMARY KEY COMMENT '任务主键UUID',
    process_instance_id VARCHAR(64) COMMENT '流程实例ID（独立任务可以为空）',
    process_id VARCHAR(64) NOT NULL COMMENT '流程定义ID',
    task_type VARCHAR(50) NOT NULL DEFAULT 'user_task' COMMENT '任务类型',
    task_def_key VARCHAR(100) NOT NULL COMMENT '节点定义ID',
    name VARCHAR(200) NOT NULL COMMENT '任务名称',
    description VARCHAR(1024) COMMENT '任务描述',
    parent_id VARCHAR(64) COMMENT '父任务ID（会签/加签场景）',
    status VARCHAR(20) NOT NULL DEFAULT 'created' COMMENT '状态：created | assigned | pending | active | delegated | suspended | completed | returned | withdrawn | terminated',
    assignee VARCHAR(64) COMMENT '当前办理人',
    owner VARCHAR(64) COMMENT '拥有人（委托前）',
    due_date DATETIME(3) COMMENT '截止时间',
    priority INT NOT NULL DEFAULT 50 COMMENT '优先级',
    form_key VARCHAR(200) COMMENT '表单Key',
    variables TEXT DEFAULT ('{}') COMMENT '任务级变量（运行时可改）',
    claimed_at DATETIME(3) COMMENT '签收时间（空=未签收）',
    sequence_order INT NOT NULL DEFAULT 0 COMMENT '会签序号（顺序会签排序，0表示主任务或非会签任务）',
    approval_type VARCHAR(20) NOT NULL DEFAULT 'single' COMMENT '审批类型：single、multi、countersign',
    approval_rule TEXT DEFAULT ('{}') COMMENT '会签规则 JSON',
    delegate_from VARCHAR(64) COMMENT '委托人ID',
    delegate_reason VARCHAR(500) COMMENT '委托原因',
    delegate_time DATETIME(3) COMMENT '委托时间',
    ended_at DATETIME(3) COMMENT '完成时间',
    comment VARCHAR(1024) COMMENT '处理意见',
    end_reason VARCHAR(1000) COMMENT '完成/终止原因',
    duration BIGINT COMMENT '耗时（毫秒）',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    created_by VARCHAR(64) NOT NULL DEFAULT '' COMMENT '创建人',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    updated_by VARCHAR(64) COMMENT '更新人',
    updated_at DATETIME(3) NULL DEFAULT NULL COMMENT '最后更新时间',
    KEY idx_task_proc_id (process_instance_id),
    KEY idx_task_assignee (assignee),
    KEY idx_task_status (status),
    KEY idx_task_asg_status (tenant_id, assignee, status),
    KEY idx_task_due (due_date),
    KEY idx_task_tenant (tenant_id),
    KEY idx_task_priority (priority DESC, created_at ASC),
    KEY idx_task_parent_sequence (parent_id, sequence_order),
    KEY idx_task_proc_def_sequence (process_instance_id, task_def_key, sequence_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='运行时任务表（仅进行中任务）';

-- 4.1 任务候选人池（存原始角色/部门引用，查询时展开）
CREATE TABLE IF NOT EXISTS wf_task_assignee (
    id VARCHAR(36) PRIMARY KEY COMMENT '主键UUID',
    task_id VARCHAR(64) NOT NULL COMMENT '任务实例ID',
    entity_type VARCHAR(20) NOT NULL DEFAULT 'role' COMMENT '实体类型：role/department/person',
    entity_id VARCHAR(64) NOT NULL COMMENT '实体ID（roleId/deptId/userId）',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    KEY idx_assignee_task (task_id),
    KEY idx_assignee_entity (entity_type, entity_id),
    KEY idx_assignee_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务候选人池（存原始角色/部门引用，查询时展开）';

-- 5. 历史任务实例表（已结束）
CREATE TABLE IF NOT EXISTS wf_hi_task (
    id VARCHAR(36) PRIMARY KEY COMMENT '任务主键UUID',
    process_instance_id VARCHAR(64) COMMENT '流程实例ID',
    process_id VARCHAR(64) NOT NULL COMMENT '流程定义ID',
    task_def_key VARCHAR(100) COMMENT '任务节点ID',
    task_type VARCHAR(50) NOT NULL DEFAULT 'user_task' COMMENT '任务类型',
    name VARCHAR(200) NOT NULL COMMENT '任务名称',
    description VARCHAR(1024) COMMENT '任务描述',
    parent_id VARCHAR(64) COMMENT '父任务ID（会签场景）',
    status VARCHAR(20) NOT NULL COMMENT '状态：created | assigned | pending | active | delegated | suspended | completed | returned | withdrawn | terminated',
    assignee VARCHAR(64) COMMENT '最终办理人',
    owner VARCHAR(64) COMMENT '任务拥有人（委托前）',
    due_date DATETIME(3) COMMENT '到期时间',
    priority INT NOT NULL DEFAULT 50 COMMENT '优先级',
    form_key VARCHAR(200) COMMENT '表单Key',
    variables TEXT DEFAULT ('{}') COMMENT '任务变量（结束瞬间快照）',
    claimed_at DATETIME(3) COMMENT '签收时间',
    sequence_order INT NOT NULL DEFAULT 0 COMMENT '会签序号（顺序会签排序，0表示主任务或非会签任务）',
    approval_type VARCHAR(20) NOT NULL DEFAULT 'single' COMMENT '审批类型：single、multi、countersign',
    approval_rule TEXT DEFAULT ('{}') COMMENT '会签规则JSON',
    delegate_from VARCHAR(64) COMMENT '委托人ID',
    delegate_reason VARCHAR(500) COMMENT '委托原因',
    delegate_time DATETIME(3) COMMENT '委托时间',
    ended_at DATETIME(3) COMMENT '完成时间',
    comment VARCHAR(1024) COMMENT '处理意见',
    end_reason VARCHAR(1000) COMMENT '完成/终止原因',
    duration BIGINT COMMENT '耗时（毫秒）',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    created_by VARCHAR(64) NOT NULL DEFAULT '' COMMENT '创建人',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    updated_by VARCHAR(64) COMMENT '更新人',
    updated_at DATETIME(3) COMMENT '最后更新时间',
    KEY idx_hi_task_proc_id (process_instance_id),
    KEY idx_hi_task_assignee (assignee),
    KEY idx_hi_task_status (status),
    KEY idx_hi_task_asg_status (tenant_id, assignee, status),
    KEY idx_hi_task_completed (ended_at DESC),
    KEY idx_hi_task_tenant (tenant_id),
    KEY idx_hi_task_duration (duration)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='历史任务实例表（已结束）';

-- 6. 任务审批意见表（任务归档后仍可读写）
CREATE TABLE IF NOT EXISTS wf_task_comment (
    id VARCHAR(36) PRIMARY KEY COMMENT '主键UUID',
    task_id VARCHAR(64) NOT NULL COMMENT '任务实例ID',
    process_instance_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '流程实例ID',
    tenant_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '租户ID',
    user_id VARCHAR(64) NOT NULL COMMENT '评论人ID',
    user_name VARCHAR(100) NOT NULL DEFAULT '' COMMENT '评论人姓名（冗余，避免联表）',
    content TEXT NOT NULL COMMENT '评论内容',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    KEY idx_comment_task (task_id),
    KEY idx_comment_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务审批意见（任务归档后仍可读写）';

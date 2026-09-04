-- ============================================================
-- GFlow 工作流表建表脚本（PostgreSQL）
--
-- 建表清单：wf_process / wf_instance / wf_hi_instance /
--           wf_task / wf_task_assignee / wf_hi_task / wf_task_comment
-- 仅包含工作流引擎自身的表；用户/角色/部门等系统表由宿主应用负责。
--
-- 用法：
--   psql -d gflow -f scripts/00.init_bpm_pg.sql
--   （或 bash scripts/init-db.sh）
--
-- 说明：
--   - 脚本幂等（CREATE TABLE/INDEX IF NOT EXISTS）：重复执行只建缺失的表，
--     不会改动已有表和数据，也不会更新已有表结构。需要重置时请重建
--     数据库；已有实例的结构升级由宿主的迁移机制负责。
--   - 字段约定：主键 VARCHAR(36)；引用列（*_id/*_by，含 tenant_id）统一
--     VARCHAR(64)；时间列 TIMESTAMPTZ（MySQL 版为 DATETIME(3)）。
--   - definition_json / variables 等 JSON 内容以 TEXT 存储，校验由应用层
--     负责（MySQL 版对应列为 JSON 类型）。
--   - 修改表结构时：同步更新两个方言脚本与宿主迁移记录（gflow 仓库
--     internal/migrations），并重新同步 gflow 仓库的 scripts/engine/ 快照。
-- ============================================================

-- 1. 流程定义表（同一租户内 process_key 按 version 递增，保留多个发布版本）
CREATE TABLE IF NOT EXISTS wf_process
(
    id                VARCHAR(36)  PRIMARY KEY,
    process_key       VARCHAR(100) NOT NULL,
    name              VARCHAR(200) NOT NULL,
    version           INTEGER      NOT NULL,
    category          VARCHAR(100),
    description       VARCHAR(500),
    definition_json   TEXT         NOT NULL,
    status            VARCHAR(20)  NOT NULL DEFAULT 'active', -- active / retired
    publish_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),    -- 版本发布时间
    icon              VARCHAR(200) NOT NULL DEFAULT '',
    process_type      VARCHAR(20)  NOT NULL DEFAULT 'main',
    /* 审计 */
    tenant_id         VARCHAR(64)  NOT NULL DEFAULT '',
    created_by        VARCHAR(64)  NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by        VARCHAR(64),
    updated_at        TIMESTAMPTZ,
    ext               TEXT         DEFAULT '{}'
);

/* 核心唯一约束：租户 + key + 版本（多租户下各租户可使用相同 process_key） */
CREATE UNIQUE INDEX IF NOT EXISTS uq_process_key_version ON wf_process (tenant_id, process_key, version);

/* 常规检索 */
CREATE INDEX IF NOT EXISTS idx_process_category   ON wf_process (category);
CREATE INDEX IF NOT EXISTS idx_process_status     ON wf_process (status);
CREATE INDEX IF NOT EXISTS idx_process_publish    ON wf_process (publish_time DESC);

/* 注释 */
COMMENT ON TABLE  wf_process IS '流程定义主表';
COMMENT ON COLUMN wf_process.id               IS 'UUID 主键';
COMMENT ON COLUMN wf_process.process_key      IS '流程键（业务唯一标识，租户内唯一）';
COMMENT ON COLUMN wf_process.name             IS '流程名称';
COMMENT ON COLUMN wf_process.version          IS '版本号（从 1 开始递增）';
COMMENT ON COLUMN wf_process.category         IS '分类（可用于权限/报表筛选）';
COMMENT ON COLUMN wf_process.description      IS '流程描述';
COMMENT ON COLUMN wf_process.definition_json  IS '流程定义DSL';
COMMENT ON COLUMN wf_process.status           IS '状态：active=生效，retired=已停用';
COMMENT ON COLUMN wf_process.publish_time     IS '版本发布时间';
COMMENT ON COLUMN wf_process.ext              IS '结构化扩展字段';
COMMENT ON COLUMN wf_process.tenant_id        IS '租户 ID';
COMMENT ON COLUMN wf_process.created_by       IS '创建人';
COMMENT ON COLUMN wf_process.created_at       IS '创建时间';
COMMENT ON COLUMN wf_process.updated_by       IS '更新人';
COMMENT ON COLUMN wf_process.updated_at       IS '更新时间';
COMMENT ON COLUMN wf_process.process_type IS '流程定义类型：main 主流程、sub 子流程';
COMMENT ON COLUMN wf_process.icon IS '流程图标';

-- 2. 运行时流程实例表
CREATE TABLE IF NOT EXISTS wf_instance
(
    id                VARCHAR(36) PRIMARY KEY,
    process_id        VARCHAR(64)  NOT NULL,
    business_key      VARCHAR(200),
    name              VARCHAR(200)  NOT NULL,
    start_user_id     VARCHAR(64)  NOT NULL DEFAULT '',
    status            VARCHAR(20)  NOT NULL DEFAULT 'active',
    variables         TEXT         DEFAULT '{}',
    current_activity  VARCHAR(100),
    priority          INTEGER      NOT NULL DEFAULT 50,
    parent_id         VARCHAR(64),

    tenant_id         VARCHAR(64)  NOT NULL DEFAULT '',
    created_by        VARCHAR(64)  NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by        VARCHAR(64),
    updated_at        TIMESTAMPTZ,

    end_reason        VARCHAR(2000),         -- 完成/终止原因（包含错误信息）
    duration          BIGINT,                -- 运行时长（毫秒）
    ended_at          TIMESTAMPTZ            -- 结束时间
);

COMMENT ON TABLE  wf_instance IS '运行时流程实例表';
COMMENT ON COLUMN wf_instance.id                IS '主键UUID';
COMMENT ON COLUMN wf_instance.process_id        IS '流程定义ID';
COMMENT ON COLUMN wf_instance.business_key      IS '业务键,业务系统唯一编号';
COMMENT ON COLUMN wf_instance.name              IS '实例名称';
COMMENT ON COLUMN wf_instance.status            IS '生命周期状态：draft / active / completed / suspended / terminated / cancelled / failed';
COMMENT ON COLUMN wf_instance.variables         IS '流程变量';
COMMENT ON COLUMN wf_instance.current_activity  IS '当前运行到的节点定义ID';
COMMENT ON COLUMN wf_instance.priority          IS '优先级（数值越大越优先）';
COMMENT ON COLUMN wf_instance.parent_id         IS '父流程实例ID（用于子流程）';
COMMENT ON COLUMN wf_instance.tenant_id         IS '租户ID（SaaS 多租户）';
COMMENT ON COLUMN wf_instance.created_by        IS '发起人用户ID';
COMMENT ON COLUMN wf_instance.created_at        IS '实例创建时间';
COMMENT ON COLUMN wf_instance.updated_by        IS '最后更新人';
COMMENT ON COLUMN wf_instance.updated_at        IS '最后更新时间';
COMMENT ON COLUMN wf_instance.end_reason IS '完成/终止原因（包含错误信息）';
COMMENT ON COLUMN wf_instance.start_user_id IS '流程发起人用户ID；系统触发时为空字符串';

/* 运行时表索引：高频查询维度 */
CREATE INDEX IF NOT EXISTS idx_instance_status        ON wf_instance (status);
CREATE INDEX IF NOT EXISTS idx_instance_tenant        ON wf_instance (tenant_id);
CREATE INDEX IF NOT EXISTS idx_instance_biz_key       ON wf_instance (business_key);
CREATE INDEX IF NOT EXISTS idx_instance_parent        ON wf_instance (parent_id);
CREATE INDEX IF NOT EXISTS idx_instance_created_at    ON wf_instance (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_instance_priority      ON wf_instance (priority DESC, created_at ASC); -- 待办/抢单排序
CREATE INDEX IF NOT EXISTS idx_instance_tenant_status ON wf_instance (tenant_id, status, created_at DESC); -- 管理端列表
CREATE INDEX IF NOT EXISTS idx_instance_tenant_starter ON wf_instance (tenant_id, start_user_id); -- 我发起的
CREATE INDEX IF NOT EXISTS idx_instance_process        ON wf_instance (process_id, created_at DESC); -- 流程定义维度的实例列表/统计

-- 3. 历史流程实例表
CREATE TABLE IF NOT EXISTS wf_hi_instance
(
    id                VARCHAR(36) PRIMARY KEY,
    process_id        VARCHAR(64)  NOT NULL,
    business_key      VARCHAR(200),
    name              VARCHAR(200)  NOT NULL,
    start_user_id     VARCHAR(64)  NOT NULL DEFAULT '',
    status            VARCHAR(20)  NOT NULL,
    variables         TEXT         DEFAULT '{}',
    current_activity  VARCHAR(100),
    priority          INTEGER      NOT NULL DEFAULT 50,
    parent_id         VARCHAR(64),
    tenant_id         VARCHAR(64)  NOT NULL DEFAULT '',
    created_by        VARCHAR(64)  NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL,
    updated_by        VARCHAR(64),
    updated_at        TIMESTAMPTZ,

    end_reason        VARCHAR(2000),         -- 完成/终止原因（包含错误信息）
    duration          BIGINT,                -- 运行时长（毫秒）
    ended_at          TIMESTAMPTZ            -- 结束时间
);

COMMENT ON TABLE  wf_hi_instance IS '历史流程实例表';
COMMENT ON COLUMN wf_hi_instance.id                IS '主键UUID';
COMMENT ON COLUMN wf_hi_instance.process_id        IS '流程定义ID';
COMMENT ON COLUMN wf_hi_instance.business_key      IS '业务键';
COMMENT ON COLUMN wf_hi_instance.name              IS '实例名称';
COMMENT ON COLUMN wf_hi_instance.status            IS '生命周期状态：draft / active / completed / suspended / terminated/cancelled/failed';
COMMENT ON COLUMN wf_hi_instance.variables         IS '流程变量';
COMMENT ON COLUMN wf_hi_instance.current_activity  IS '最后停留节点';
COMMENT ON COLUMN wf_hi_instance.priority          IS '优先级';
COMMENT ON COLUMN wf_hi_instance.parent_id         IS '父流程实例ID';
COMMENT ON COLUMN wf_hi_instance.tenant_id         IS '租户ID';
COMMENT ON COLUMN wf_hi_instance.created_by        IS '发起人';
COMMENT ON COLUMN wf_hi_instance.created_at        IS '实例创建时间';
COMMENT ON COLUMN wf_hi_instance.updated_by        IS '最后更新人';
COMMENT ON COLUMN wf_hi_instance.updated_at        IS '最后更新时间';
COMMENT ON COLUMN wf_hi_instance.end_reason IS '完成/终止原因（包含错误信息）';
COMMENT ON COLUMN wf_hi_instance.duration          IS '运行时长（毫秒）';
COMMENT ON COLUMN wf_hi_instance.ended_at          IS '流程结束时间';
COMMENT ON COLUMN wf_hi_instance.start_user_id IS '流程发起人用户ID；系统触发时为空字符串';

/* 历史表索引：报表/审计/清理 维度 */
CREATE INDEX IF NOT EXISTS idx_hist_instance_tenant    ON wf_hi_instance (tenant_id);
CREATE INDEX IF NOT EXISTS idx_hist_instance_status    ON wf_hi_instance (status);
CREATE INDEX IF NOT EXISTS idx_hist_instance_biz_key   ON wf_hi_instance (business_key);
CREATE INDEX IF NOT EXISTS idx_hist_instance_parent    ON wf_hi_instance (parent_id);
CREATE INDEX IF NOT EXISTS idx_hist_instance_created   ON wf_hi_instance (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_hist_instance_ended     ON wf_hi_instance (ended_at DESC);
CREATE INDEX IF NOT EXISTS idx_hist_instance_duration  ON wf_hi_instance (duration);
CREATE INDEX IF NOT EXISTS idx_hist_inst_tenant_status ON wf_hi_instance (tenant_id, status, created_at DESC); -- 管理端历史列表
CREATE INDEX IF NOT EXISTS idx_hist_inst_tenant_starter ON wf_hi_instance (tenant_id, start_user_id); -- 我发起的(历史)
CREATE INDEX IF NOT EXISTS idx_hist_instance_process    ON wf_hi_instance (process_id, created_at DESC); -- 流程定义维度的历史实例列表

-- 4. 运行时任务表  wf_task  —— 只保留“进行中”需要更新的字段
CREATE TABLE IF NOT EXISTS wf_task
(
    id                  VARCHAR(36) PRIMARY KEY,
    process_instance_id VARCHAR(64),                -- 独立任务可以为空
    process_id          VARCHAR(64) NOT NULL,
    task_type           VARCHAR(50) NOT NULL DEFAULT 'user_task',
    task_def_key        VARCHAR(100) NOT NULL,      -- 节点定义ID
    name                VARCHAR(200) NOT NULL,
    description         VARCHAR(1024),
    parent_id           VARCHAR(64),                -- 加签父任务
    status              VARCHAR(20) NOT NULL DEFAULT 'created', -- created | assigned | pending | active | delegated | suspended | completed | returned | withdrawn | terminated
    assignee            VARCHAR(64),                -- 当前办理人
    owner               VARCHAR(64),                -- 拥有人（委托前）
    due_date            TIMESTAMPTZ,                -- 截止时间
    priority            INTEGER NOT NULL DEFAULT 50,
    form_key            VARCHAR(200),
    variables           TEXT DEFAULT '{}',          -- 任务级变量（运行时可改）
    claimed_at          TIMESTAMPTZ,                -- 签收时间（空=未签收）
    sequence_order      INTEGER NOT NULL DEFAULT 0,
    /* 审批相关 */
    approval_type       VARCHAR(20) NOT NULL DEFAULT 'single',
    approval_rule       TEXT DEFAULT '{}',          -- 会签规则 JSON
    /* 委托相关 */
    delegate_from       VARCHAR(64),
    delegate_reason     VARCHAR(500),
    delegate_time       TIMESTAMPTZ,

    /* 结束相关字段 */
    ended_at            TIMESTAMPTZ,                -- 完成时间
    comment             VARCHAR(1024),              -- 处理意见
    end_reason          VARCHAR(1000),              -- 完成/终止原因
    duration            BIGINT,                     -- 耗时（毫秒）

    /* 审计 */
    tenant_id           VARCHAR(64) NOT NULL DEFAULT '',
    created_by          VARCHAR(64) NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(64),
    updated_at          TIMESTAMPTZ
);

/* ------------ 运行时表索引 ------------ */
CREATE INDEX IF NOT EXISTS idx_task_proc_id    ON wf_task (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_task_assignee   ON wf_task (assignee);
CREATE INDEX IF NOT EXISTS idx_task_status     ON wf_task (status);
CREATE INDEX IF NOT EXISTS idx_task_asg_status ON wf_task (tenant_id, assignee, status); -- 待办/已办按办理人
CREATE INDEX IF NOT EXISTS idx_task_due        ON wf_task (due_date);
CREATE INDEX IF NOT EXISTS idx_task_tenant     ON wf_task (tenant_id);
CREATE INDEX IF NOT EXISTS idx_task_priority   ON wf_task (priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_task_parent_sequence ON wf_task (parent_id, sequence_order) WHERE parent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_proc_def_sequence ON wf_task (process_instance_id, task_def_key, sequence_order);

/* ------------ 注释 ------------ */
COMMENT ON TABLE wf_task IS '运行时任务表（仅进行中任务）';
COMMENT ON COLUMN wf_task.id                  IS '任务主键UUID';
COMMENT ON COLUMN wf_task.process_instance_id IS '流程实例ID';
COMMENT ON COLUMN wf_task.task_def_key        IS '任务节点ID';
COMMENT ON COLUMN wf_task.task_type           IS '任务类型';
COMMENT ON COLUMN wf_task.parent_id           IS '父任务ID（会签场景）';
COMMENT ON COLUMN wf_task.status              IS '状态：created | assigned | pending | active | delegated | suspended | completed | returned | withdrawn | terminated';
COMMENT ON COLUMN wf_task.assignee            IS '当前办理人';
COMMENT ON COLUMN wf_task.owner               IS '任务拥有人（委托前）';
COMMENT ON COLUMN wf_task.due_date            IS '到期时间';
COMMENT ON COLUMN wf_task.variables           IS '任务变量';
COMMENT ON COLUMN wf_task.claimed_at          IS '签收时间';
COMMENT ON COLUMN wf_task.sequence_order IS '会签序号（用于顺序会签排序，0表示主任务或非会签任务）';
COMMENT ON COLUMN wf_task.ended_at            IS '完成时间';
COMMENT ON COLUMN wf_task.approval_type       IS '审批类型：single、multi、countersign';
COMMENT ON COLUMN wf_task.approval_rule       IS '会签规则JSON';
COMMENT ON COLUMN wf_task.delegate_from       IS '委托人ID';
COMMENT ON COLUMN wf_task.delegate_time       IS '委托时间';
COMMENT ON COLUMN wf_task.tenant_id           IS '租户ID';
COMMENT ON COLUMN wf_task.created_at          IS '创建时间';
COMMENT ON COLUMN wf_task.updated_at          IS '最后更新时间';

-- 4.1 任务候选人池  wf_task_assignee  —— 存原始角色/部门引用，查询时展开
CREATE TABLE IF NOT EXISTS wf_task_assignee
(
    id          VARCHAR(36) PRIMARY KEY,
    task_id     VARCHAR(64) NOT NULL,                 -- 关联 wf_task.id
    entity_type VARCHAR(20) NOT NULL DEFAULT 'role',  -- role | department | person
    entity_id   VARCHAR(64) NOT NULL,                 -- roleId / deptId / userId
    tenant_id   VARCHAR(64) NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assignee_task   ON wf_task_assignee (task_id);
CREATE INDEX IF NOT EXISTS idx_assignee_entity ON wf_task_assignee (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_assignee_tenant ON wf_task_assignee (tenant_id);

COMMENT ON TABLE  wf_task_assignee IS '任务候选人池（存原始角色/部门引用，查询时展开）';
COMMENT ON COLUMN wf_task_assignee.id          IS '主键UUID';
COMMENT ON COLUMN wf_task_assignee.task_id     IS '任务实例ID';
COMMENT ON COLUMN wf_task_assignee.entity_type IS '实体类型：role/department/person';
COMMENT ON COLUMN wf_task_assignee.entity_id   IS '实体ID（roleId/deptId/userId）';
COMMENT ON COLUMN wf_task_assignee.tenant_id   IS '租户ID';
COMMENT ON COLUMN wf_task_assignee.created_at  IS '创建时间';

-- 5. 历史任务表  wf_hi_task
CREATE TABLE IF NOT EXISTS wf_hi_task
(
    id                  VARCHAR(36) PRIMARY KEY,
    process_instance_id VARCHAR(64),
    process_id          VARCHAR(64) NOT NULL,
    task_def_key        VARCHAR(100),
    task_type           VARCHAR(50) NOT NULL DEFAULT 'user_task',
    name                VARCHAR(200) NOT NULL,
    description         VARCHAR(1024),
    parent_id           VARCHAR(64),
    status              VARCHAR(20) NOT NULL,
    assignee            VARCHAR(64),                          -- 最终办理人
    owner               VARCHAR(64),
    due_date            TIMESTAMPTZ,
    priority            INTEGER NOT NULL DEFAULT 50,
    form_key            VARCHAR(200),
    variables           TEXT DEFAULT '{}',                    -- 结束瞬间快照
    claimed_at          TIMESTAMPTZ,
    sequence_order      INTEGER NOT NULL DEFAULT 0,
    /* 审批/委托快照 */
    approval_type       VARCHAR(20) NOT NULL DEFAULT 'single',
    approval_rule       TEXT DEFAULT '{}',
    delegate_from       VARCHAR(64),
    delegate_reason     VARCHAR(500),
    delegate_time       TIMESTAMPTZ,
    /* 结束相关字段 */
    ended_at            TIMESTAMPTZ,                          -- 完成时间
    comment             VARCHAR(1024),                        -- 处理意见
    end_reason          VARCHAR(1000),                        -- 完成/终止原因
    duration            BIGINT,                               -- 耗时（毫秒）
    /* 审计 */
    tenant_id           VARCHAR(64) NOT NULL DEFAULT '',
    created_by          VARCHAR(64) NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL,
    updated_by          VARCHAR(64),
    updated_at          TIMESTAMPTZ
);

/* ------------ 历史表索引 ------------ */
CREATE INDEX IF NOT EXISTS idx_hi_task_proc_id    ON wf_hi_task (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_hi_task_assignee   ON wf_hi_task (assignee);
CREATE INDEX IF NOT EXISTS idx_hi_task_status     ON wf_hi_task (status);
CREATE INDEX IF NOT EXISTS idx_hi_task_asg_status ON wf_hi_task (tenant_id, assignee, status); -- 已办按办理人
CREATE INDEX IF NOT EXISTS idx_hi_task_completed  ON wf_hi_task (ended_at DESC);
CREATE INDEX IF NOT EXISTS idx_hi_task_tenant     ON wf_hi_task (tenant_id);
CREATE INDEX IF NOT EXISTS idx_hi_task_duration   ON wf_hi_task (duration);

/* ------------ 注释 ------------ */
COMMENT ON TABLE wf_hi_task IS '历史任务实例表（已结束）';
COMMENT ON COLUMN wf_hi_task.id                  IS '任务主键UUID';
COMMENT ON COLUMN wf_hi_task.process_instance_id IS '流程实例ID';
COMMENT ON COLUMN wf_hi_task.process_id IS '流程定义ID';
COMMENT ON COLUMN wf_hi_task.name IS '任务名称';
COMMENT ON COLUMN wf_hi_task.task_def_key        IS '任务节点ID';
COMMENT ON COLUMN wf_hi_task.task_type           IS '任务类型';
COMMENT ON COLUMN wf_hi_task.parent_id           IS '父任务ID（会签场景）';
COMMENT ON COLUMN wf_hi_task.status              IS '状态：created | assigned | pending | active | delegated | suspended | completed | returned | withdrawn | terminated';
COMMENT ON COLUMN wf_hi_task.assignee            IS '办理人';
COMMENT ON COLUMN wf_hi_task.owner               IS '任务拥有人（委托前）';
COMMENT ON COLUMN wf_hi_task.due_date            IS '到期时间';
COMMENT ON COLUMN wf_hi_task.form_key            IS '表单Key';
COMMENT ON COLUMN wf_hi_task.variables           IS '任务变量（结束瞬间快照）';
COMMENT ON COLUMN wf_hi_task.claimed_at          IS '签收时间';
COMMENT ON COLUMN wf_hi_task.sequence_order IS '会签序号（用于顺序会签排序，0表示主任务或非会签任务）';
COMMENT ON COLUMN wf_hi_task.approval_type       IS '审批类型：single、multi、countersign';
COMMENT ON COLUMN wf_hi_task.approval_rule       IS '会签规则JSON';
COMMENT ON COLUMN wf_hi_task.delegate_from       IS '委托人ID';
COMMENT ON COLUMN wf_hi_task.delegate_time       IS '委托时间';
COMMENT ON COLUMN wf_hi_task.ended_at            IS '完成时间';
COMMENT ON COLUMN wf_hi_task.comment             IS '处理意见';
COMMENT ON COLUMN wf_hi_task.end_reason          IS '完成/终止原因';
COMMENT ON COLUMN wf_hi_task.duration            IS '任务耗时（毫秒）';
COMMENT ON COLUMN wf_hi_task.tenant_id           IS '租户ID';
COMMENT ON COLUMN wf_hi_task.created_at          IS '创建时间';
COMMENT ON COLUMN wf_hi_task.updated_at          IS '最后更新时间';

-- 6. 任务审批意见表  wf_task_comment
CREATE TABLE IF NOT EXISTS wf_task_comment
(
    id                  VARCHAR(36) PRIMARY KEY,
    task_id             VARCHAR(64) NOT NULL,                 -- 关联 wf_task.id（任务归档后仍可评论/查询）
    process_instance_id VARCHAR(64) NOT NULL DEFAULT '',
    tenant_id           VARCHAR(64) NOT NULL DEFAULT '',
    user_id             VARCHAR(64) NOT NULL,                 -- 评论人ID
    user_name           VARCHAR(100) NOT NULL DEFAULT '',     -- 评论人姓名（冗余，避免联表）
    content             TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_comment_task   ON wf_task_comment (task_id);
CREATE INDEX IF NOT EXISTS idx_comment_tenant ON wf_task_comment (tenant_id);

COMMENT ON TABLE  wf_task_comment IS '任务审批意见（任务归档后仍可读写）';
COMMENT ON COLUMN wf_task_comment.task_id IS '任务实例ID';
COMMENT ON COLUMN wf_task_comment.process_instance_id IS '流程实例ID';
COMMENT ON COLUMN wf_task_comment.tenant_id IS '租户ID';
COMMENT ON COLUMN wf_task_comment.user_id IS '评论人ID';
COMMENT ON COLUMN wf_task_comment.user_name IS '评论人姓名';
COMMENT ON COLUMN wf_task_comment.content IS '评论内容';
COMMENT ON COLUMN wf_task_comment.created_at IS '创建时间';

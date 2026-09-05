# GFlow Engine

[![GoDoc](https://pkg.go.dev/badge/github.com/rulego/gflow-engine)](https://pkg.go.dev/github.com/rulego/gflow-engine)
[![Go Report](https://goreportcard.com/badge/github.com/rulego/gflow-engine)](https://goreportcard.com/report/github.com/rulego/gflow-engine)

English | [简体中文](README.md)

> **GFlow** — AI pre-screens · Humans approve · Automation follows
>
> **GFlow Engine** — the embeddable approval workflow engine at the core of the GFlow product family (open-source edition).
> It ships as a Go library: no UI, no HTTP server — you drive it from your own application.
>
> Need something ready to run? **GFlow Platform（极风工作流）** is the GFlow Enterprise Edition,
> with a flow designer, form designer, approval UI and AI review built in.
> Site: <https://gflow.rulego.cc/en/> · Live demo: <http://8.134.32.225:8081> (`admin` / `admin123`)

`GFlow Engine` is a lightweight, embeddable approval workflow engine built on [RuleGo](https://github.com/rulego/rulego). Process definitions reuse the `RuleGo` rule-chain DSL (JSON), while tasks, process instances and history are persisted to a relational database by the engine itself — no separate process middleware to deploy. Approval nodes and automation nodes (rule chains, HTTP calls, AI agents, sub-processes) live in the same process DSL — downstream actions run automatically once approved; Chinese-style approval semantics (or-sign, countersign, dynamic add/remove signers, return) work out of the box.

> Note: the DSL is a BPMN-style approval flow in JSON form; it does not parse BPMN 2.0 XML.

## Features

* **Rule chain as process, approval + automation in one DSL**
  * **Rule chain as process:** the DSL is a `RuleGo` rule chain. Native nodes such as gateways (`switch`) and parallel branches (`fork`/`inclusive`/`join`) can be mixed into approval flows directly.
  * **Approval and automation in one chain:** `serviceTask` calls Go functions, `automation` invokes `RuleGo` rule chains, `aiAgent` talks to agent rule chains ([rulego-components-ai](https://github.com/rulego/rulego-components-ai)), and `httpCall` performs synchronous HTTP calls.
  * **Sub-processes:** the `subProcess` node starts an independent child instance and returns to the parent flow on completion.
* **Chinese-style approval semantics & process model**
  * **Chinese-style approval semantics:** single sign-off, countersign (parallel/sequential, all/majority), dynamic add/remove signers, transfer, delegation, claim, return, withdraw, suspend/resume, overdue handling — out of the box, no extra development.
  * **Candidate-group tasks:** tasks can be offered to users, roles or departments; the candidate pool (`wf_task_assignee`) is stored separately and expanded through `IdentityService` at query time.
  * **Approval comments:** task comments live in `wf_task_comment` and survive task archival; approval actions record their comment in the same transaction.
  * **Versioned definitions:** each `process_key` keeps multiple published versions; running instances continue on the version they started with.
* **Integration & extension**
  * **Pluggable identity:** implement `IdentityService` to resolve approvers from your real user/role/department data (by role, department, group or manager hierarchy). The built-in mock is for tests only.
  * **Event hooks:** 17 task lifecycle events dispatched after commit (full catalog in [docs/events.md](docs/events.md)); register one or many listeners via the builder. Platform hooks: cross-tenant overdue scan (`ScanOverdueTasks`) and batch claimable-instance lookup (`GetClaimableInstanceIDs`).
  * **Pluggable SQL dialects:** PostgreSQL / MySQL out of the box; register others (SQLite, Dameng, Kingbase, ...) via `DialectProvider` — see the examples directory.
* **Architecture & deployment**
  * **Runtime/history split:** in-flight data and archived data live in separate tables, so reporting and audits never contend with the hot path.
  * **Multi-tenancy:** `tenant_id` isolation end to end, with per-tenant rule-engine pools.
  * **Lightweight:** no mandatory external middleware (a local in-memory lock is built in; distributed locking is pluggable). Fits embedding into existing applications.

## Installation

```bash
go get github.com/rulego/gflow-engine
```

Requires Go 1.24+ and RuleGo v0.37+. Initialize the workflow tables with `scripts/00.init_bpm_pg.sql` (PostgreSQL) or `scripts/00.init_bpm_mysql.sql` (MySQL); initialization and upgrade guidance lives in [docs/migration.md](docs/migration.md).

## Database setup

The engine manages only its own workflow tables; users, roles and departments belong to the host application.

**PostgreSQL:**

```bash
createdb gflow   # create the database first (or: psql -c "CREATE DATABASE gflow")
psql -d gflow -f scripts/00.init_bpm_pg.sql
```

**MySQL:**

```bash
mysql -u root -p -e "CREATE DATABASE gflow DEFAULT CHARACTER SET utf8mb4"
mysql -u root -p gflow < scripts/00.init_bpm_mysql.sql
```

Tables created: `wf_process` (definitions), `wf_instance` / `wf_hi_instance` (runtime/history instances), `wf_task` / `wf_hi_task` (runtime/history tasks), `wf_task_assignee` (candidate pool), `wf_task_comment` (approval comments).

> The init scripts are idempotent (`CREATE TABLE IF NOT EXISTS`) — re-running them on an existing database never deletes or modifies data. Unit tests run on an in-memory SQLite database and need no scripts. The optional real-database lock test (`TestWithInstanceTx_RealDB_ForUpdateSerializes`) runs only when `TEST_PG_DSN` or `TEST_MYSQL_DSN` points to an existing database; it creates and drops its own `gflow_locktest` scratch database.

## Quick start

Run the complete example before integrating it yourself (deploy → start → approve → inspect instance status in one go, zero-dependency on in-memory SQLite):

```bash
go run ./examples/leave_approval
```

Minimal integration code:

```go
package main

import (
	"context"
	"log"

	"github.com/rulego/gflow-engine/components"
	"github.com/rulego/gflow-engine/config"
	"github.com/rulego/gflow-engine/model"
	"github.com/rulego/gflow-engine/service"
)

func main() {
	ctx := context.Background()

	// 1. Configure the database and start the engine
	cfg := &config.Config{
		Database: &config.DatabaseConfig{
			Driver: "postgres",
			Dsn:    "host=127.0.0.1 user=postgres password=postgres dbname=gflow port=5432 sslmode=disable",
		},
	}
	engine, err := service.NewWorkflowEngineBuilder().
		SetName("demo").
		SetConfig(cfg).
		SetIDGenerator(service.NewIDGenerator()).
		Build()
	if err != nil {
		log.Fatalf("build engine: %v", err)
	}
	if err := engine.Start(ctx); err != nil {
		log.Fatalf("start engine: %v", err)
	}
	defer engine.Stop(ctx)

	// 2. Register workflow node components (userTask/serviceTask/automation/...)
	//    Dependencies are pulled from the engine itself; register your host service
	//    functions via WithServiceFuncs — see docs/components.md.
	//    Note: engine.Start must have succeeded first, otherwise registration
	//    fails fast with an error.
	if err := components.RegisterFromEngine(engine); err != nil {
		log.Fatalf("register components: %v", err)
	}

	// 3. Deploy a process definition (the DSL is a rulego rule chain JSON,
	//    see examples/leave_approval). All mutating operations take an explicit
	//    actor — the operator identity used for auditing and permission checks.
	admin := service.Actor{UserID: "admin", UserName: "admin", TenantID: "default"}
	_, err = engine.GetProcessService().Deploy(ctx, admin, &model.WfProcess{
		ProcessKey:     "leave_approval",
		Name:           "Leave Approval",
		DefinitionJSON: leaveApprovalDSL, // process DSL JSON; see examples/leave_approval/dsl.json for reference
		TenantID:       "default",
		CreatedBy:      "admin",
	}, true)
	if err != nil {
		log.Fatalf("deploy process: %v", err)
	}

	// 4. Start a process instance (pass service.WithDraft() as a variadic
	//    option to start in draft mode; omit it for a normal start)
	instanceID, err := engine.GetRuntimeService().StartProcessInstanceByKey(
		ctx,
		service.Actor{UserID: "emp001", UserName: "Zhang San", TenantID: "default"},
		"leave_approval",
		"leave_emp001_1", // business key
		map[string]interface{}{"days": 5, "managerId": "mgr001", "reason": "family affairs"},
	)
	if err != nil {
		log.Fatalf("start instance: %v", err)
	}
		log.Printf("instance started: %s", instanceID)
}
```

Handling a pending task (also import `github.com/rulego/gflow-engine/types/dto` and `.../types/enums`):

```go
tasks, _, err := engine.GetTaskService().GetTaskList(ctx, service.Actor{
	UserID:   "mgr001",
	TenantID: "default",
}, &dto.TaskQuery{
	Assignee: "mgr001",
	PageRequest: dto.PageRequest{
		Status:   []string{string(enums.TaskStatusPending), string(enums.TaskStatusActive)},
		PageSize: 10,
	},
})
if err != nil || len(tasks) == 0 {
	return
}

err = engine.GetTaskService().CompleteWithApproval(ctx, service.Actor{
	UserID:   "mgr001",
	TenantID: "default",
}, &service.ApprovalRequest{
	TaskID:         tasks[0].ID,
	ApprovalResult: enums.ApprovalResultApproved,
	Comment:        "approved",
})
```

> **Security note — the operator is the explicit `actor Actor` parameter:** all approval
> and mutation operations (`CompleteWithApproval`, `Approve`, `Reject`, `Claim`, `Unclaim`,
> `Transfer`, `Reassign`, `Return`, `Withdraw`, `WithdrawByInstance`, `DeleteTask`, ...)
> receive the operator as an explicit `actor service.Actor` parameter; the engine checks
> assignee permission against `actor.UserID`. A call with no operator identity is rejected
> with `ErrAuthenticationRequired`, and an operator who is not the task assignee gets
> `ErrPermissionDenied`. Construct the `Actor` on the server side from your authentication
> layer (session/token) — never trust client input directly.

A complete runnable example (single sign-off, parallel and sequential countersign) lives in [examples/leave_approval](examples/leave_approval) — it runs zero-dependency on an in-memory SQLite database by default (`GFLOW_DSN` switches to PostgreSQL/MySQL). An `httpCall` + `switch` combination example (query an external API, map the response into process variables, route by the result) lives in [examples/http_call](examples/http_call). The engine ships with an in-memory mock identity service for tests only — inject your own `IdentityService` as described in the next section for production.

## Identity integration (organizational data)

The engine does not bind to any user system. Tasks offered to roles, departments or managers resolve their assignees through the `service.IdentityService` interface — in production you must inject an implementation backed by your own user/role/department tables (the built-in in-memory mock is for tests only):

```go
// Implement all 9 methods of service.IdentityService against your org tables
type OrgIdentityService struct {
	db *gorm.DB // host application data source
}

// Users by role (role candidate tasks); the other methods follow the same pattern
func (s *OrgIdentityService) GetUserIDsByRoleID(ctx context.Context, tenantID, roleID string) ([]string, error) {
	var userIDs []string
	err := s.db.WithContext(ctx).
		Table("user_roles").
		Where("tenant_id = ? AND role_id = ?", tenantID, roleID).
		Pluck("user_id", &userIDs).Error
	return userIDs, err
}

// Remaining methods and their purpose:
//   GetUserIDsByDepartmentID        users by department (dept candidate tasks)
//   GetDepartmentManagerUserID      department manager (dept candidate tasks)
//   GetUserManagerID                direct manager (direct_manager candidate tasks)
//   GetUserManagerHierarchy         manager hierarchy (multi_level_manager candidate tasks)
//   GetUserDepartmentID             department of a user
//   GetRoleIDsByUserID              roles of a user (todo visibility for role candidates)
//   GetDepartmentIDsByUserID        departments of a user (todo visibility for dept candidates)
//   GetUserIDsByGroupID             users by custom group (reserved extension)
```

Inject it through the builder:

```go
engine, err := service.NewWorkflowEngineBuilder().
	SetName("demo").
	SetConfig(cfg).
	SetIdentityService(&OrgIdentityService{db: gormDB}).
	Build()
```

Mapping from candidate configuration (`candidateType`) to interface methods:

| candidateType | Resolving methods |
|---|---|
| `user` | none (user IDs given directly via `candidateUsers`) |
| `role` | `GetUserIDsByRoleID`; todo visibility via `GetRoleIDsByUserID` |
| `dept` | `GetUserIDsByDepartmentID` / `GetDepartmentManagerUserID`; todo visibility via `GetDepartmentIDsByUserID` |
| `direct_manager` | `GetUserManagerID` |
| `multi_level_manager` | `GetUserManagerHierarchy` |
| `initiator_select` / `initiator_self` | none (chosen by initiator / the initiator) |

> Optional hardening: if the implementation also implements `TenantMembershipChecker` (`IsUserInTenant`), the engine verifies that transfer/delegation/reassign targets belong to the task's tenant and blocks cross-tenant reassignment; without it the check is skipped (with a warning logged).

## Process DSL

A process definition is a `RuleGo` rule chain: `nodes` describe the steps, `connections` describe the flow, and approval configuration lives in each node's `configuration`. The BPM extension nodes:

| Node type | Description |
|---|---|
| `startTask` | Start node: marks the process entry point; no auth checks (handled by the initiator layer) |
| `userTask` | Human approval task: single/countersign, candidates (user/role/department), form (`formKey` passed through to `wf_task.form_key`), priority, due date; `configuration.taskName`/`taskDescription` override the node name/description |
| `ccTask` | CC task: creates a CC record and notifies the host app via `CCTaskCreatedListener` |
| `serviceTask` | Service task: invokes a Go function (registered via `action.Functions`) |
| `automation` | Automation node: invokes a `RuleGo` rule chain |
| `subProcess` | Sub-process: starts an independent child instance, then returns to the parent flow |
| `aiAgent` | AI agent node: invokes an agent rule chain and routes the output |
| `httpCall` | HTTP call: synchronous external request, response merged into process variables |
| `startProcess` | Rule-chain-only: starts a BPM process instance from within a rule chain (e.g. scheduled auto-start of approvals) |

Native `RuleGo` nodes (`switch` gateways, `fork`/`inclusive`/`join` parallel joins, ...) can be used directly — see the [RuleGo standard components](https://rulego.cc/pages/standard-components/).

For the full node reference (configuration fields, approval modes, reject strategies, `httpCall` SSRF protection) see [docs/components.md](docs/components.md). The complete documentation set for embedders — task lifecycle, events, persistence model — is indexed in [docs/README.md](docs/README.md).

## Extension points

* **Identity:** implement `service.IdentityService` to resolve approvers by role, department, group or manager hierarchy.
* **SQL dialect:** implement `service.DialectProvider` to support additional databases — see [examples/custom_dialect](examples/custom_dialect) (Dameng, Kingbase).
* **Distributed locking:** the engine ships a built-in local in-memory lock (`lock.NewLocalLock`). For multi-instance deployments, implement the `lock.Locker` interface (e.g. Redis `SET NX` with a Lua-script release) and inject it via `WorkflowEngineBuilder.SetLocker`.
* **Task events:** `TaskEventListener` / `CCTaskCreatedListener` receive task lifecycle events to drive notifications and other side effects — see [docs/events.md](docs/events.md) for the full event catalog, payload fields and integration guide.

## Ecosystem

- [RuleGo](https://github.com/rulego/rulego) — the underlying rule engine
- Documentation: <https://gflow.rulego.cc/en/>
- GFlow Engine source: [Gitee](https://gitee.com/rulego/gflow-engine) · [GitHub](https://github.com/rulego/gflow-engine)
- **GFlow Platform**（极风工作流）— the GFlow Enterprise Edition, a ready-to-run approval platform built on GFlow Engine (backend + UI + designers); site: <https://gflow.rulego.cc/>, live demo at <http://8.134.32.225:8081>

## Contact & commercial licensing

GFlow Engine is free and open source (Apache-2.0). If you need the Enterprise Edition —
**GFlow Platform**（极风工作流）with source-code delivery (flow/form designers, approval UI,
AI review) — or commercial support, see <https://gflow.rulego.cc/> or reach us through
any of these channels (please mention your purpose when adding):

- QQ: [2215016127](tencent://message/?uin=2215016127&Site=&Menu=yes)
- WeChat: `rulegoteam`
- Email: [rulego@outlook.com](mailto:rulego@outlook.com)

## License

`GFlow Engine` is licensed under Apache 2.0. See the [LICENSE](LICENSE) file for details.

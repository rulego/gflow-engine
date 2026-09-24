package e2e

import (
	"testing"
	"time"

	"github.com/rulego/gflow-engine/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 委派归还事件回归：委派后被委派人 approve，任务应归还原审批人（不完成不
// 流转），且 TaskEventResolved 必须派发（宿主依赖它给原审批人发"归还了"
// 通知）。事件派发异步，用 recorder 轮询等待。
func TestE2E_DelegateResolveEmitsResolvedEvent(t *testing.T) {
	env := newE2EEnv(t)
	e2eEvents.reset()

	env.deploySimpleProcess("dlg_resolved_e2e", "Delegate Resolved", "single",
		[]string{"a_user"}, nil)
	instID := env.startInstance("dlg_resolved_e2e", "starter")

	require.Eventually(t, func() bool {
		return len(env.activeTasksFor(instID, "a_user")) == 1
	}, 2*time.Second, 20*time.Millisecond, "a_user 应有任务")
	tasks := env.activeTasksFor(instID, "a_user")
	require.Len(t, tasks, 1, "a_user 应有任务")
	taskID := tasks[0].ID

	// 委派给 b_user
	ctx := env.userCtxAs("a_user")
	require.NoError(t, env.engine.GetTaskService().Delegate(ctx,
		service.Actor{UserID: "a_user", TenantID: e2eTenantID}, taskID, "b_user", "代审"))

	// 被委派人通过 → 归还而非流转
	bTasks := env.activeTasksFor(instID, "b_user")
	require.Len(t, bTasks, 1, "被委派人应持有任务")
	env.approveAs(bTasks[0].ID, "b_user", "代审完毕")

	// 任务回到 a_user，实例仍在运行
	back := env.activeTasksFor(instID, "a_user")
	assert.Len(t, back, 1, "任务应归还 a_user")
	assert.Empty(t, env.activeTasksFor(instID, "b_user"), "被委派人不再持有任务")

	evs := e2eEvents.waitFor(t, service.TaskEventResolved, 3*time.Second)
	var resolved []service.TaskEvent
	for _, evt := range evs {
		if evt.Type == service.TaskEventResolved {
			resolved = append(resolved, evt)
		}
	}
	require.NotEmpty(t, resolved, "委派归还必须派发 TaskEventResolved（宿主归还通知依赖）")
	assert.Equal(t, "a_user", resolved[0].ToUsers[0], "Resolved 事件的接收人应是原审批人")
}

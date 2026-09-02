package components

import (
	"sync"

	"github.com/rulego/gflow-engine/service"
)

// globalAttachmentResolver 包级附件解析器，AIAgentNode 组装多模态输入时引用。
// 由 register.go 经 SetAttachmentResolver 注入；读写经锁保护，运行期替换安全。
var (
	attachmentResolverMu     sync.RWMutex
	globalAttachmentResolver service.AttachmentResolver
)

// SetAttachmentResolver 注入附件解析器。nil 合法（未注入时 aiAgent 节点
// 保持纯文本附件行为）。
func SetAttachmentResolver(r service.AttachmentResolver) {
	attachmentResolverMu.Lock()
	defer attachmentResolverMu.Unlock()
	globalAttachmentResolver = r
}

// getAttachmentResolver 读取当前附件解析器，可能为 nil。
func getAttachmentResolver() service.AttachmentResolver {
	attachmentResolverMu.RLock()
	defer attachmentResolverMu.RUnlock()
	return globalAttachmentResolver
}

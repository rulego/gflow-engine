package service

// AttachmentRef 附件的规范化引用。引擎从流程变量 / metadata 提取附件清单后
// 归一化为该结构，交给宿主注入的 AttachmentResolver 解析。
type AttachmentRef struct {
	// Name 附件显示名（含扩展名），如 "发票.png"
	Name string
	// URL 下载地址（上传组件写入，可能是相对路径，如 /uploads/{tenant}/.../f.png）
	URL string
	// Path 存储路径（存储键，优先于 URL；上传组件写入，可能为空）
	Path string
}

// ResolvedAttachment 附件解析结果。
// 图片：Source 为模型可用的图片来源（服务器本地绝对路径 / 公网 URL / base64
// data URL）；本地路径在送模型前压缩转 base64，非视觉模型自动降级为路径文本。
// 文档：Text 为抽取的纯文本。
type ResolvedAttachment struct {
	Name string
	// Source 图片来源；仅图片类解析结果填写
	Source string
	// Text 文档抽取的文本；仅文档类解析结果填写
	Text string
}

// AttachmentResolver 附件解析器：把流程附件解析成 LLM 可直接消费的形态。
// 由宿主（持有文件存储的一方）实现并注入引擎；nil 时 aiAgent 节点保持
// 纯文本行为（附件仅以文件名+地址进提示词），完全向后兼容。
// 实现要求：单个附件解析失败只跳过该附件并记录日志，不让整体失败。
type AttachmentResolver interface {
	// ResolveImages 解析图片类附件（按扩展名过滤是解析器的职责）
	ResolveImages(tenantID string, refs []AttachmentRef) []ResolvedAttachment
	// ResolveDocs 解析文档类附件（PDF/TXT/MD 等可抽文本的格式）
	ResolveDocs(tenantID string, refs []AttachmentRef) []ResolvedAttachment
}

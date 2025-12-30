# 架构
- General 通用Agent(直接处理简单任务、规划)
- Explorer 搜索Agent(搜索文本、文件、网页等)
- Executor 执行Agent(可同时创建多个，并行执行任务)

# 资源管理
## 状态
General 总状态
SubAgent 各自独立状态
## 消息
General 拥有全部消息
每个 SubAgent 获取 General 发送的部分消息，SubAgent 完成任务后只返回 General 最终结果，或将最终结果写入总状态


# 工作流程
General 加载技能元数据，接收用户输入

# 技能
https://platform.claude.com/docs/zh-CN/agents-and-tools/agent-skills/overview
## 渐进式披露
1. 技能元数据(始终加载)
SKILL.md 文件最顶端 yaml 格式的元数据，包含 name、description
2. 技能详细信息(触发时加载)
SKILL.md 文件所有内容
3. 资源与代码(按需加载)
附加技能目录或文件，通过 SKILL.md 中的名称引用

# 权限隔离
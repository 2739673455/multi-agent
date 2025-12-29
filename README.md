# 架构
- General 通用Agent
- Planner 规划Agent
- Explorer 搜索Agent
- Executor 执行Agent

# 工作流程
General 接收用户输入，并读取技能元数据

# 技能
https://platform.claude.com/docs/zh-CN/agents-and-tools/agent-skills/overview
## 渐进式披露
1. 技能元数据(始终加载)
SKILL.md 文件最顶端 yaml 格式的元数据，包含 name、description
2. 技能详细信息(触发时加载)
SKILL.md 文件所有内容
3. 资源与代码(按需加载)
附加技能目录或文件，通过 SKILL.md 中的名称引用

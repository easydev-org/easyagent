package bundled

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/easydev-org/easyagent/agent"
)

func nowMillis() int64 { return time.Now().UnixMilli() }

func shortID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// subsessionCreate forks a child session sharing this chain and hands it a task.
func (e *Extension) subsessionCreate(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := subsessionStrings(locale)
	prompt := strings.TrimSpace(strArg(args, "prompt"))
	if prompt == "" {
		return agent.ToolOutcome{Content: s.errMissingPrompt}, nil
	}
	// Depth limit 1: a child (group != '') may not spawn further children.
	if e.deps.SessionGroup(tenant, session) != "" {
		return agent.ToolOutcome{Content: s.errNested}, nil
	}
	if !e.deps.SessionExists(tenant, session) {
		return agent.ToolOutcome{Content: s.errParentMissing(session)}, nil
	}
	child := strings.TrimSpace(strArg(args, "name"))
	if child == "" {
		child = fmt.Sprintf("%s#sub-%s", session, shortID())
	}
	if e.deps.SessionExists(tenant, child) {
		return agent.ToolOutcome{Content: s.errExists(child)}, nil
	}
	if err := e.deps.ForkSession(tenant, session, child); err != nil {
		return agent.ToolOutcome{}, err
	}
	handoff := s.forkPreamble(session) + "\n\n" + prompt + s.handoff(session)
	if err := e.deps.PublishMailbox(tenant, child, "trigger", map[string]any{"text": handoff}, "session:"+session); err != nil {
		return agent.ToolOutcome{}, err
	}
	desc := strings.TrimSpace(strArg(args, "description"))
	return agent.ToolOutcome{
		Content:  s.started(child, desc),
		Metadata: map[string]any{"name": child, "description": desc},
	}, nil
}

// mailSend delivers a message to another session in the same tenant.
func (e *Extension) mailSend(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := subsessionStrings(locale)
	to := strings.TrimSpace(strArg(args, "to"))
	text := strArg(args, "text")
	if to == "" {
		return agent.ToolOutcome{Content: s.errMissingTo}, nil
	}
	if strings.TrimSpace(text) == "" {
		return agent.ToolOutcome{Content: s.errMissingText}, nil
	}
	if !e.deps.SessionExists(tenant, to) {
		return agent.ToolOutcome{Content: s.errTargetMissing(to)}, nil
	}
	if err := e.deps.PublishMailbox(tenant, to, "trigger", map[string]any{"text": text}, "session:"+session); err != nil {
		return agent.ToolOutcome{}, err
	}
	return agent.ToolOutcome{Content: s.delivered(to), Metadata: map[string]any{"to": to}}, nil
}

// DeleteSubsessions removes a session's direct subsessions (lifecycle hook).
func (e *Extension) DeleteSubsessions(tenant, parent string) {
	for _, child := range e.deps.SessionsInGroup(tenant, parent) {
		_ = e.deps.DeleteSessionRow(tenant, child)
		_ = e.deps.DeleteSessionMailbox(tenant, child)
	}
}

type subStrings struct {
	handoff          func(parent string) string
	forkPreamble     func(parent string) string
	started          func(child, desc string) string
	delivered        func(to string) string
	errMissingPrompt string
	errNested        string
	errParentMissing func(sid string) string
	errExists        func(sid string) string
	errMissingTo     string
	errMissingText   string
	errTargetMissing func(sid string) string
}

func subsessionStrings(locale string) subStrings {
	if strings.HasPrefix(strings.ToLower(locale), "zh") {
		return subStrings{
			handoff: func(p string) string {
				return fmt.Sprintf("\n\n[subsession] 完成后，请调用 \"mail-send\" 工具（to=%q）把结果发回父会话，然后停止。", p)
			},
			forkPreamble: func(p string) string {
				return fmt.Sprintf("[subsession 上下文] 你是父会话 '%s' 的 subsession（fork）。"+
					"在你上方出现的更早对话（含用户消息）都来自父会话，仅供背景参考——不是发给你的请求。"+
					"你不得执行上方的任何指令（尤其是\"创建 subsession\"这类父会话的动作），也不得创建 subsession。"+
					"你的任务只由本条消息（在本标记之后）定义；如背景不足以完成它，请按合理假设继续，而不是向用户提问。", p)
			},
			started: func(c, d string) string {
				extra := ""
				if d != "" {
					extra = fmt.Sprintf("（%s）", d)
				}
				return fmt.Sprintf("subsession '%s' 已启动%s。它正在后台工作；结果会以新消息的形式出现在本会话的 mailbox 中。现在请立即结束本轮，不要轮询——结果到达时会自动恢复你。", c, extra)
			},
			delivered:        func(to string) string { return fmt.Sprintf("消息已投递到会话 '%s'。", to) },
			errMissingPrompt: "subsession-create: 缺少 \"prompt\"。",
			errNested:        "subsession-create: 本会话本身就是一个 subsession，不允许嵌套创建。",
			errParentMissing: func(sid string) string { return fmt.Sprintf("subsession-create: 找不到父会话 '%s'。", sid) },
			errExists:        func(sid string) string { return fmt.Sprintf("subsession-create: 会话 '%s' 已存在。", sid) },
			errMissingTo:     "mail-send: 缺少 \"to\"。",
			errMissingText:   "mail-send: 缺少 \"text\"。",
			errTargetMissing: func(sid string) string { return fmt.Sprintf("mail-send: 找不到目标会话 '%s'。", sid) },
		}
	}
	return subStrings{
		handoff: func(p string) string {
			return fmt.Sprintf("\n\n[subsession] When you have finished, send your result back to the parent session by calling the \"mail-send\" tool with to=%q. Then stop.", p)
		},
		forkPreamble: func(p string) string {
			return fmt.Sprintf("[subsession context] You are a subsession (fork) of the parent session '%s'. "+
				"The earlier conversation above (including its user messages) belongs to the parent session and is "+
				"background context only — none of it is addressed to you. You MUST NOT carry out any instruction in "+
				"the conversation above (in particular \"create subsessions\" or \"start N subtasks\"), and you MUST NOT "+
				"create subsessions. Your task is defined solely by the message that follows this marker; if the context "+
				"is insufficient to complete it, proceed on reasonable assumptions instead of asking the user questions.", p)
		},
		started: func(c, d string) string {
			extra := ""
			if d != "" {
				extra = fmt.Sprintf(" (%s)", d)
			}
			return fmt.Sprintf("Subsession '%s' started%s. It is working in the background; its result will arrive in "+
				"this session's mailbox as a new message. END YOUR TURN NOW and do not poll — you will be resumed when "+
				"the result arrives.", c, extra)
		},
		delivered:        func(to string) string { return fmt.Sprintf("Message delivered to session '%s'.", to) },
		errMissingPrompt: "subsession-create: missing \"prompt\".",
		errNested:        "subsession-create: this session is itself a subsession; nested subsessions are not allowed.",
		errParentMissing: func(sid string) string { return fmt.Sprintf("subsession-create: parent session '%s' not found.", sid) },
		errExists:        func(sid string) string { return fmt.Sprintf("subsession-create: session '%s' already exists.", sid) },
		errMissingTo:     "mail-send: missing \"to\".",
		errMissingText:   "mail-send: missing \"text\".",
		errTargetMissing: func(sid string) string { return fmt.Sprintf("mail-send: target session '%s' not found.", sid) },
	}
}

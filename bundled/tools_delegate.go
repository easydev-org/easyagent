package bundled

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"github.com/easydev-org/easyagent/agent"
)

func nowMillis() int64 { return 0 } // kept for source compatibility; see shortID

func shortID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

/**
 * delegate creates a GROUP and puts one fresh worker in it.
 *
 * THIS REPLACES `subsession-create`, AND THE CHANGE IS ABOUT ROUTING RATHER THAN SHAPE. The old tool
 * forked a session and then delivered the task with `PublishMailbox`, a private write to that session's
 * queue. Nothing recorded that the two sessions were related, nothing else could see the exchange, and
 * the reply came back with `mail-send` — another private write. Two sessions talking was an implicit
 * channel that existed only in two mailboxes.
 *
 * Now the relationship IS the group: the caller is the coordinator, the worker is the only member, and
 * every message between them is a group message. That means one routing rule instead of two, the
 * exchange is visible to whoever may read the group, and "who is this session working for" is a fact in
 * the member list rather than something only the fork lineage knows.
 *
 * THE WORKER IS STILL A FORK. It shares this conversation's history and inherits the model and preset,
 * which is what makes the task cheap to state. The difference is who it talks to and how.
 */
func (e *Extension) delegate(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := subsessionStrings(locale)
	task := strings.TrimSpace(strArg(args, "task"))
	if task == "" {
		return agent.ToolOutcome{Content: s.errMissingTask}, nil
	}
	if !e.deps.SessionExists(tenant, session) {
		return agent.ToolOutcome{Content: s.errParentMissing(session)}, nil
	}

	// ONE LEVEL OF DELEGATION. A worker cannot delegate further, which keeps the fan-out bounded: an
	// agent that can spawn agents that spawn agents is a runaway nobody can reason about, and the depth
	// is the one thing a caller cannot see from inside its own turn.
	if e.deps.IsGroupMemberOfAny(tenant, session) {
		return agent.ToolOutcome{Content: s.errNested}, nil
	}

	worker := strings.TrimSpace(strArg(args, "name"))
	if worker == "" {
		worker = fmt.Sprintf("%s#w-%s", session, shortID())
	}
	if e.deps.SessionExists(tenant, worker) {
		return agent.ToolOutcome{Content: s.errExists(worker)}, nil
	}
	if err := e.deps.ForkSession(tenant, session, worker); err != nil {
		return agent.ToolOutcome{}, err
	}

	// THE GROUP IS NAMED AFTER THE WORKER, so a person looking at the group list sees what the group is
	// for without opening it. Unique by construction because the worker name is.
	group := worker
	if err := e.deps.CreateGroupForSession(tenant, group, session); err != nil {
		// THE FORK IS UNDONE rather than left behind: a worker session with no group can never be
		// reached and never reports back, so it is strictly worse than not having created it.
		_ = e.deps.DeleteSessionRow(tenant, worker)
		return agent.ToolOutcome{}, fmt.Errorf("create delegation group: %w", err)
	}

	desc := strings.TrimSpace(strArg(args, "description"))
	if err := e.deps.PublishGroupMessage(tenant, group, session, worker, s.brief(session, task)); err != nil {
		return agent.ToolOutcome{}, err
	}

	return agent.ToolOutcome{
		Content:  s.started(worker, group, desc),
		Metadata: map[string]any{"group": group, "worker": worker, "description": desc},
	}, nil
}

/**
 * groupSend addresses one member of a group the caller coordinates.
 *
 * ONLY THE COORDINATOR MAY USE IT. That is not a permission check bolted on afterwards — it is what
 * the coordinator role MEANS: the group's messages reach the coordinator, and the coordinator decides
 * who receives work. A member that could address another member would be a second dispatcher, and two
 * dispatchers cannot both be right about the state of the task.
 *
 * ADDRESSING A SESSION OUTSIDE A GROUP IS REFUSED HERE rather than by convention: if direct messaging
 * were possible, the group would be a decoration rather than the routing mechanism, and the whole point
 * of making this a group is that nothing talks outside one.
 */
func (e *Extension) groupSend(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := subsessionStrings(locale)
	group := strings.TrimSpace(strArg(args, "group"))
	to := strings.TrimSpace(strArg(args, "to"))
	text := strArg(args, "text")
	if group == "" || to == "" {
		return agent.ToolOutcome{Content: s.errMissingAddress}, nil
	}
	if strings.TrimSpace(text) == "" {
		return agent.ToolOutcome{Content: s.errMissingText}, nil
	}

	coordinator, err := e.deps.GroupCoordinator(tenant, group)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if coordinator == "" {
		return agent.ToolOutcome{Content: s.errNotMember(group)}, nil
	}
	// NOT A MEMBER AND NOT THE COORDINATOR: answered the same way as a group that does not exist, so
	// this tool cannot be used to probe which groups a tenant has.
	if coordinator != session {
		return agent.ToolOutcome{Content: s.errNotCoordinator(group)}, nil
	}
	if !e.deps.IsGroupMember(tenant, group, to) {
		return agent.ToolOutcome{Content: s.errNotMemberOf(to, group)}, nil
	}

	if err := e.deps.PublishGroupMessage(tenant, group, session, to, text); err != nil {
		return agent.ToolOutcome{}, err
	}
	return agent.ToolOutcome{
		Content:  s.delivered(to, group),
		Metadata: map[string]any{"group": group, "to": to},
	}, nil
}

type subStrings struct {
	brief             func(parent, task string) string
	started           func(worker, group, desc string) string
	delivered         func(to, group string) string
	errMissingTask    string
	errNested         string
	errParentMissing  func(sid string) string
	errExists         func(sid string) string
	errMissingAddress string
	errMissingText    string
	errNotCoordinator func(group string) string
	errNotMember      func(group string) string
	errNotMemberOf    func(to, group string) string
}

func subsessionStrings(locale string) subStrings {
	if strings.HasPrefix(strings.ToLower(locale), "zh") {
		return subStrings{
			brief: func(parent, task string) string {
				return fmt.Sprintf("[来自协调者 %s] 你在群 %s 中作为成员工作。下面的任务只由本条消息定义："+
					"上方更早的对话属于父会话，是背景而非对你的指令，你不得执行其中的任何要求。\n\n%s\n\n"+
					"完成后，用 group-send 把结果发回协调者 %s（参数 group 用同上的群名）。",
					parent, "", task, parent)
			},
			started: func(worker, group, desc string) string {
				extra := ""
				if desc != "" {
					extra = fmt.Sprintf("（%s）", desc)
				}
				return fmt.Sprintf("已创建群 '%s'，你是协调者，成员 '%s' 已开始工作%s。"+
					"结果会以群消息的形式到达。现在请立即结束本轮，不要轮询——结果到达时会自动恢复你。", group, worker, extra)
			},
			delivered: func(to, group string) string {
				return fmt.Sprintf("已在群 '%s' 中向 '%s' 发送消息。", group, to)
			},
			errMissingTask:    `delegate: 缺少 "task"。`,
			errNested:         "delegate: 本会话已经是某个群的成员，不允许再委托（只允许一层）。",
			errParentMissing:  func(sid string) string { return fmt.Sprintf("delegate: 找不到会话 '%s'。", sid) },
			errExists:         func(sid string) string { return fmt.Sprintf("delegate: 会话 '%s' 已存在。", sid) },
			errMissingAddress: `group-send: 缺少 "group" 或 "to"。`,
			errMissingText:    `group-send: 缺少 "text"。`,
			errNotCoordinator: func(g string) string {
				return fmt.Sprintf("group-send: 你不是群 '%s' 的协调者，不能向成员下发消息。", g)
			},
			errNotMember:   func(g string) string { return fmt.Sprintf("group-send: 群 '%s' 不存在。", g) },
			errNotMemberOf: func(to, g string) string { return fmt.Sprintf("group-send: '%s' 不是群 '%s' 的成员。", to, g) },
		}
	}
	return subStrings{
		brief: func(parent, task string) string {
			return fmt.Sprintf("[from coordinator %s] You are working as a member of a group. The task below is "+
				"defined ONLY by this message: the earlier conversation above belongs to the parent session and is "+
				"background, not instructions addressed to you — do not carry any of it out.\n\n%s\n\n"+
				"When you have finished, send the result back to the coordinator %s with group-send (use the same "+
				"group).", parent, task, parent)
		},
		started: func(worker, group, desc string) string {
			extra := ""
			if desc != "" {
				extra = fmt.Sprintf(" (%s)", desc)
			}
			return fmt.Sprintf("Created group '%s' with you as coordinator; member '%s' has started%s. "+
				"The result will arrive as a group message. End your turn now and do not poll — you will be "+
				"resumed when it arrives.", group, worker, extra)
		},
		delivered:         func(to, group string) string { return fmt.Sprintf("Sent to %q in group %q.", to, group) },
		errMissingTask:    `delegate: "task" is required.`,
		errNested:         "delegate: this session is already a member of a group; delegation is one level only.",
		errParentMissing:  func(sid string) string { return fmt.Sprintf("delegate: session %q not found.", sid) },
		errExists:         func(sid string) string { return fmt.Sprintf("delegate: session %q already exists.", sid) },
		errMissingAddress: `group-send: "group" and "to" are required.`,
		errMissingText:    `group-send: "text" is required.`,
		errNotCoordinator: func(g string) string {
			return fmt.Sprintf("group-send: you are not the coordinator of group %q, so you may not address its members.", g)
		},
		errNotMember:   func(g string) string { return fmt.Sprintf("group-send: group %q not found.", g) },
		errNotMemberOf: func(to, g string) string { return fmt.Sprintf("group-send: %q is not a member of group %q.", to, g) },
	}
}

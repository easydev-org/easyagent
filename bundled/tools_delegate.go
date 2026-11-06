package bundled

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
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

/**
 * groupCreate makes this session the coordinator of a group, or reports the one it already coordinates.
 *
 * ONE COORDINATORSHIP PER SESSION, and the failure mode is an ANSWER rather than an error: "you already
 * coordinate X" is far more useful to a model than a refusal, because the next thing it wants is that
 * name — to list the members, or to address one of them.
 *
 * THE GROUP NAME IS DERIVED FROM THE SESSION, not chosen by the model, so a session cannot collide with
 * another session's group by picking a name. Two sessions with the same name cannot exist in a tenant,
 * so the derived name is unique by construction.
 */
func (e *Extension) groupCreate(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := groupStringsFor(locale)

	/**
	 * A SESSION MAY CREATE ANY NUMBER OF GROUPS, so a name is now REQUIRED.
	 *
	 * It used to default to the session's own name, which worked only because a session could coordinate exactly
	 * one group: the name was unique by construction. With several, the default would collide on the second call —
	 * and "the group called after me" is not a meaningful name once there is more than one.
	 *
	 * CREATING A GROUP WITH A NAME THAT ALREADY EXISTS IS STILL REFUSED, so a model that lost track gets a
	 * definite answer rather than a duplicate.
	 */
	name := strings.TrimSpace(strArg(args, "name"))
	if name == "" {
		return agent.ToolOutcome{Content: s.errMissingName}, nil
	}
	id, err := e.deps.CreateGroupForSession(tenant, name, session)
	if err != nil {
		if isGroupExists(err) {
			return agent.ToolOutcome{Content: s.errGroupTaken(name)}, nil
		}
		return agent.ToolOutcome{}, err
	}

	/**
	 * THE ANSWER CARRIES BOTH: the id to address with, the name to read.
	 *
	 * Every tool that takes a group takes the ID (`group-send`, `group-members`, `spawn`), and a model that is told
	 * only the name it just chose has nothing to call next. Saying both is what makes the very next step possible
	 * without a lookup.
	 */
	return agent.ToolOutcome{
		Content:  s.created(name, id),
		Metadata: map[string]any{"group": id, "name": name, "created": true},
	}, nil
}

/**
 * spawn forks this session into a worker and puts it in the coordinator's group, then hands it a task.
 *
 * THE WORKER LOSES `group.create`. That is the whole reason this is a separate tool from a plain fork:
 * a worker that could create groups could spawn workers of its own, and the delegation depth would be
 * whatever the model felt like. Withholding one permission makes the depth exactly one, and it is
 * enforced by the same gate that guards every other privileged tool — not by a counter here.
 *
 * THE WORKER IS STILL A FULL SESSION. It has this conversation's history, this model and preset, and
 * every other grant — it simply cannot start a group, which means it cannot recruit.
 *
 * IT WORKS EVEN WHEN THE CALLER CANNOT CREATE GROUPS. A worker spawning a helper is the ordinary case;
 * what matters is that the worker is already IN a group, because that group is where the helper goes.
 */
func (e *Extension) spawn(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := groupStringsFor(locale)
	task := strings.TrimSpace(strArg(args, "task"))
	if task == "" {
		return agent.ToolOutcome{Content: s.errMissingTask}, nil
	}
	if !e.deps.SessionExists(tenant, session) {
		return agent.ToolOutcome{Content: s.errParentMissing(session)}, nil
	}

	/**
	 * THE WORKER GOES INTO A GROUP THIS SESSION COORDINATES — and in no other case.
	 *
	 * A MEMBER CANNOT SPAWN. Being in a group is not authority over it: a member that could add
	 * sessions would be a second way in, past the person who owns the group, and the coordinator would
	 * find workers it never asked for in its member list.
	 *
	 * A SESSION THAT IS A MEMBER OF ONE GROUP MAY STILL SPAWN — as the coordinator of ANOTHER group it
	 * created itself. That is not an exception to the rule: the worker joins the group the spawner
	 * coordinates, and has no relationship at all with the group it merely belongs to.
	 */
	/**
	 * THE GROUP TO SPAWN INTO IS NAMED BY THE CALLER.
	 *
	 * It used to be derived: a session coordinated exactly one group, so there was nothing to choose. With
	 * several, deriving it would put a worker into whichever group happened to sort first — a silent misfiling
	 * that nobody would notice until the wrong team was doing the work.
	 *
	 * The name is checked against the groups this session actually coordinates, so a model cannot spawn into a
	 * group it has no authority over even if it guesses the name.
	 */
	coordinated, err := e.deps.CoordinatedGroups(tenant, session)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if len(coordinated) == 0 {
		return agent.ToolOutcome{Content: s.errNoCoordinatedGroup}, nil
	}
	/**
	 * THE GROUP IS ALWAYS NAMED, even when there is only one.
	 *
	 * A DEFAULT IS A DECISION THE CALLER DID NOT MAKE. "The only one I have" happens to be unambiguous today, but
	 * the tool is what a model reads when it composes the call — and a parameter that is sometimes optional teaches
	 * a habit that breaks the first time there are two. Requiring it means the model always says which group it is
	 * filing work under, and the refusal when it names a group it does not coordinate is a real check rather than a
	 * fallback nobody exercised.
	 */
	group := strings.TrimSpace(strArg(args, "group"))
	if group == "" {
		return agent.ToolOutcome{Content: s.errSpawnNeedsGroup(coordinated)}, nil
	}
	if !slices.Contains(coordinated, group) {
		return agent.ToolOutcome{Content: s.errNotCoordinatorOf(group)}, nil
	}

	/**
	 * `name` IS A LABEL, AND THE ID COMES BACK FROM THE FORK.
	 *
	 * Every step below — adding to the group, addressing it, deleting it on failure — needs the ID, not
	 * the label: the id is what the group stores, what the channel subject carries and what a later
	 * `group-send` must name. Using the label would have produced a group member that resolves to
	 * nothing and a task published to a subject nobody subscribes to.
	 */
	name := strings.TrimSpace(strArg(args, "name"))
	if name == "" {
		name = fmt.Sprintf("worker-%s", shortID())
	}
	// THE MASK IS APPLIED DURING THE COPY, not by revoking afterwards: a revoking loop leaves a window
	// where the worker holds the grant, and a tool call in that window would be authorised by exactly
	// the permission this is trying to withhold.
	worker, err := e.deps.ForkSession(tenant, session, name, []string{
		"bundled/group.create", "bundled/group.spawn",
	})
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if err := e.deps.AddGroupMemberOnly(tenant, group, worker); err != nil {
		_ = e.deps.DeleteSessionRow(tenant, worker)
		return agent.ToolOutcome{}, fmt.Errorf("add worker to group: %w", err)
	}

	desc := strings.TrimSpace(strArg(args, "description"))
	// A BRIEF HAS NO FILES: it is the task description that wakes the worker, and the worker's first move is to
	// look at the group. Attachments belong to a message someone composed, not to a spawn.
	posted, err := e.deps.PostGroupMessage(tenant, group, session, []string{worker}, s.brief(session, group, task), nil)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	/**
	 * THE WORKER OWES A REPLY, and that is the point of `spawn`: the coordinator hands over a task and then waits. A
	 * worker that reports back is the normal case; one that finishes quietly is the failure this makes visible.
	 */
	if err := e.deps.OpenReplyObligations(tenant, group, session, []string{worker}, posted.Seq, posted.MessageID); err != nil {
		return agent.ToolOutcome{}, err
	}

	return agent.ToolOutcome{
		Content:  s.spawned(name, worker, group, desc),
		Metadata: map[string]any{"group": group, "worker": worker, "name": name, "description": desc},
	}, nil
}

/**
 * groupSend addresses one member of a group.
 *
 * TWO DIRECTIONS ARE ALLOWED AND ONE IS NOT:
 *
 *   coordinator → member   hand out work
 *   member      → coordinator   report back
 *   member      → member   REFUSED
 *
 * The refusal is what keeps the group from becoming a mesh: a member that could address another member
 * would be a second dispatcher, and two dispatchers cannot both be right about the state of the task.
 * Reporting upward is not a second dispatcher — the coordinator already knows what it handed out, and
 * a result with nowhere to go would make every delegation a dead end.
 *
 * NOBODY OUTSIDE THE GROUP can be reached at all, which is what makes "every session-to-session message
 * goes through a group" true rather than advisory.
 */
func (e *Extension) groupSend(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	s := groupStringsFor(locale)
	group := strings.TrimSpace(strArg(args, "group"))
	text := strArg(args, "text")

	/**
	 * `to` IS AN ARRAY BECAUSE "THE SAME THING, TO SEVERAL PEOPLE" IS ONE DECISION.
	 *
	 * A coordinator that needs three workers to hear the same instruction should say so once: three
	 * separate calls would be three chances to write three slightly different texts, and the recipients
	 * would disagree about what they were told. This is NOT a broadcast — every name is still resolved,
	 * validated and published individually, so the set of people who hear it is exactly the set the
	 * caller named.
	 */
	to, err := strListArg(args, "to")
	if err != nil {
		return agent.ToolOutcome{Content: s.errBadTo}, nil
	}
	/**
	 * ATTACHMENTS ARE BARE BLOB CODES — the same `file:<code>` identity a chat message carries.
	 *
	 * A coordinator that produced a picture, a report or a recording has a code from the tool that made it, and
	 * handing that code to the group is the whole operation: the bytes already live in the tenant's blob store,
	 * so nothing is copied and every member reads the same file. There is deliberately no framing step here —
	 * a code either names an ingested blob or it names nothing.
	 */
	attachments, err := strListArg(args, "attachments")
	if err != nil {
		return agent.ToolOutcome{Content: s.errBadAttachments}, nil
	}
	if group == "" {
		return agent.ToolOutcome{Content: s.errMissingGroup}, nil
	}
	/**
	 * A MESSAGE NEEDS SOMETHING IN IT: words, a file, or both.
	 *
	 * "Here is the chart" and the chart itself are one message, and so is the chart alone. Requiring text would
	 * make a coordinator invent a caption to send a picture it was asked for.
	 */
	if strings.TrimSpace(text) == "" && len(attachments) == 0 {
		return agent.ToolOutcome{Content: s.errMissingText}, nil
	}

	coordinator, err := e.deps.GroupCoordinator(tenant, group)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if coordinator == "" {
		return agent.ToolOutcome{Content: s.errNotMember(group)}, nil
	}
	// THE SENDER MUST BE IN THE GROUP. Answered the same way as a missing group, so this tool cannot be
	// used to probe which groups the tenant has.
	senderIsMember := coordinator == session || e.deps.IsGroupMember(tenant, group, session)
	if !senderIsMember {
		return agent.ToolOutcome{Content: s.errNotMember(group)}, nil
	}

	/**
	 * EVERY RECIPIENT IS VALIDATED BEFORE ANYTHING IS SENT.
	 *
	 * A HALF-DELIVERED INSTRUCTION IS WORSE THAN A REFUSED ONE: some workers would start on a task the
	 * others never received, and nothing in the transcript says which. So the whole list is checked
	 * first and the sends only begin once every name is a member the sender may address.
	 */
	for _, rcpt := range to {
		if !e.deps.IsGroupMember(tenant, group, rcpt) {
			return agent.ToolOutcome{Content: s.errNotMemberOf(rcpt, group)}, nil
		}
		// THE ONE REFUSED DIRECTION: member to member. A member may only address the coordinator, so a
		// list containing anyone else is refused whole.
		if coordinator != session && rcpt != coordinator {
			return agent.ToolOutcome{Content: s.errPeerToPeer(group)}, nil
		}
		if rcpt == session {
			return agent.ToolOutcome{Content: s.errSelfAddress}, nil
		}
	}

	if len(to) == 0 {
		/**
		 * NO `to` MEANS A REPLY TO THE GROUP, NOT AN ERROR.
		 *
		 * A coordinator's answer to what a user asked IN the group belongs in that group; leaving it in the
		 * coordinator's own chat means the person who asked never sees it. An empty `to` is therefore the
		 * most common call, not a mistake — and treating it as one is why a live group showed the question
		 * and no answer at all.
		 *
		 * NOTHING IS DELIVERED TO MEMBERS: a reply is for the group's reader, not an instruction. Waking
		 * three workers to read "收到，我来汇总" would cost three turns and produce three more replies.
		 */
		if _, err := e.deps.PostGroupMessage(tenant, group, session, nil, text, attachments); err != nil {
			return agent.ToolOutcome{}, err
		}
		/**
		 * A BARE REPLY SETTLES NOTHING. An obligation is discharged only by ADDRESSING the session that asked, which
		 * is the one form of "I answered you" that cannot be confused with talking to the room: a message to the
		 * group is seen by everyone and confirms nothing to anyone in particular.
		 */
		return agent.ToolOutcome{
			Content:  s.replied(group),
			Metadata: map[string]any{"group": group},
		}, nil
	}

	for _, rcpt := range to {
		posted, err := e.deps.PostGroupMessage(tenant, group, session, []string{rcpt}, text, attachments)
		if err != nil {
			return agent.ToolOutcome{}, err
		}
		/**
		 * BEING NAMED IS A DEBT. The coordinator asked this member something, and "did it answer" is a question the
		 * group page has to be able to answer later — which is why the obligation carries the seq of THIS message.
		 */
		if err := e.deps.OpenReplyObligations(tenant, group, session, []string{rcpt}, posted.Seq, posted.MessageID); err != nil {
			return agent.ToolOutcome{}, err
		}
		/**
		 * AND IT MAY SETTLE ONE. Addressing `rcpt` is exactly the form a reply takes, so whatever this session owed
		 * that recipient is now answered. Both happen in one call: a message can be an answer to what it was asked
		 * and a question to whoever it names next.
		 */
		if err := e.deps.FulfilReplyObligations(tenant, group, session, rcpt); err != nil {
			return agent.ToolOutcome{}, err
		}
	}
	return agent.ToolOutcome{
		Content:  s.delivered(to, group),
		Metadata: map[string]any{"group": group, "to": to},
	}, nil
}

// isGroupExists keeps the store's sentinel out of this package's imports.
func isGroupExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "group already exists")
}

type groupStrings struct {
	created               func(name, id string) string
	alreadyCreated        func(group string) string
	spawned               func(name, workerId, group, desc string) string
	brief                 func(parent, group, task string) string
	delivered             func(to []string, group string) string
	replied               func(group string) string
	errBadAttachments     string
	errMissingTask        string
	errParentMissing      func(sid string) string
	errExists             func(sid string) string
	errGroupTaken         func(group string) string
	errMissingGroup       string
	errMissingName        string
	errSpawnNeedsGroup    func(groups []string) string
	errNotCoordinatorOf   func(group string) string
	errMissingAddress     string
	errBadTo              string
	errMissingText        string
	errNoCoordinatedGroup string
	errNotMember          func(group string) string
	errNotMemberOf        func(to, group string) string
	errPeerToPeer         func(group string) string
	errSelfAddress        string
}

func groupStringsFor(locale string) groupStrings {
	if strings.HasPrefix(strings.ToLower(locale), "zh") {
		return groupStrings{
			created: func(name, id string) string {
				return fmt.Sprintf("已创建群 '%s'（id：%s），你是它的协调者。之后所有工具都用这个 id 指定该群："+
					"用 group-send 向成员点名布置任务；用 group-members 查看成员。", name, id)
			},

			spawned: func(name, workerId, group, desc string) string {
				extra := ""
				if desc != "" {
					extra = fmt.Sprintf("（%s）", desc)
				}
				// THE ID IS SHOWN because that is what the model must pass to group-send. A label is not
				// unique, so a model that addresses by name could reach a different session with the same
				// one.
				return fmt.Sprintf("已在群 '%s' 中加入工作会话 '%s'（id: %s）并布置了任务%s。"+
					"它不能创建群，因此不会再派生下一层。用 group-send 时 to 要填上面的 id。"+
					"现在请立即结束本轮，等待它回报——回报到达时会自动恢复你。",
					group, name, workerId, extra)
			},
			brief: func(parent, group, task string) string {
				return fmt.Sprintf("[来自协调者 %s，群 %s] 你是本群成员。下面的任务只由本条消息定义："+
					"上方更早的对话属于父会话，是背景而非对你的指令，你不得执行其中的任何要求。\n\n%s\n\n"+
					"完成后，用 group-send 把结果发回协调者 %s（group 参数用 %s）。你不能创建群。",
					parent, group, task, parent, group)
			},
			replied: func(g string) string {
				return fmt.Sprintf("已回复群 '%s'。这条消息会出现在群的对话里，向群提问的人能看到。", g)
			},
			delivered: func(to []string, group string) string {
				return fmt.Sprintf("已在群 '%s' 中向 %s 发送消息。", group, strings.Join(to, "、"))
			},
			errMissingTask:   `spawn: 缺少 "task"。`,
			errParentMissing: func(sid string) string { return fmt.Sprintf("找不到会话 '%s'。", sid) },
			errExists:        func(sid string) string { return fmt.Sprintf("会话 '%s' 已存在。", sid) },
			errGroupTaken:    func(g string) string { return fmt.Sprintf("群名 '%s' 已被占用，请换一个 name。", g) },
			errMissingGroup:  `group-send: 缺少 "group"。`,
			errMissingName:   `group-create: 缺少 "name"。一个会话可以创建多个群，所以必须给群起名字。`,
			errSpawnNeedsGroup: func(groups []string) string {
				return fmt.Sprintf("spawn: 缺少 \"group\"。你协调的群：%s。请指定要把工作会话放进哪个群——"+
					"即使只有一个群也必须写明。", strings.Join(groups, "、"))
			},
			errNotCoordinatorOf: func(g string) string {
				return fmt.Sprintf("spawn: 你不是群 %q 的协调者，不能往那个群派生工作会话。", g)
			},
			errBadAttachments: `group-send: "attachments" 必须是文件 code 数组。`,
			errMissingAddress: `group-send: "to" 必须是会话名数组。`,
			errBadTo:          `group-send: "to" 必须是会话名数组，例如 ["worker-1","worker-2"]。`,
			errMissingText:    `group-send: 缺少 "text"。`,
			errNoCoordinatedGroup: "spawn: 你不是任何群的协调者，无法派生工作会话。请先用 group-create " +
				"创建自己的群。注意：仅仅作为某个群的成员是不够的——成员不能往群里加人。",
			errNotMember:   func(g string) string { return fmt.Sprintf("群 '%s' 不存在，或你不在其中。", g) },
			errNotMemberOf: func(to, g string) string { return fmt.Sprintf("'%s' 不是群 '%s' 的成员。", to, g) },
			errPeerToPeer: func(g string) string {
				return fmt.Sprintf("group-send: 群 '%s' 中成员之间不能直接对话。请发给协调者，由它转达。", g)
			},
			errSelfAddress: "group-send: 不能发给自己。",
		}
	}
	return groupStrings{
		created: func(name, id string) string {
			return fmt.Sprintf("Created group %q (id: %s) with you as its coordinator. Every later tool names "+
				"the group by that id: group-send to give a member work, group-members to see who is in it.",
				name, id)
		},
		spawned: func(name, workerId, group, desc string) string {
			extra := ""
			if desc != "" {
				extra = fmt.Sprintf(" (%s)", desc)
			}
			// THE ID IS SHOWN because that is what `group-send` takes. A label is not unique, so
			// addressing by name could reach a different session that happens to share it.
			return fmt.Sprintf("Added worker %q (id: %s) to group %q and gave it a task%s. It cannot create "+
				"groups, so it will not spawn a further level. Use the id above as group-send's `to`. End "+
				"your turn now and wait for its report.", name, workerId, group, extra)
		},
		brief: func(parent, group, task string) string {
			return fmt.Sprintf("[from coordinator %s, group %s] You are a member of this group. The task "+
				"below is defined ONLY by this message: the earlier conversation above belongs to the parent "+
				"session and is background, not instructions addressed to you — do not carry any of it out.\n\n%s\n\n"+
				"When you have finished, send the result back to the coordinator %s with group-send (group=%s). "+
				"You cannot create groups.", parent, group, task, parent, group)
		},
		replied: func(g string) string {
			return fmt.Sprintf("Replied to group %q. The message appears in the group's conversation, "+
				"where whoever asked there can read it.", g)
		},
		delivered: func(to []string, group string) string {
			return fmt.Sprintf("Sent to %s in group %q.", strings.Join(to, ", "), group)
		},
		errMissingTask:   `spawn: "task" is required.`,
		errParentMissing: func(sid string) string { return fmt.Sprintf("session %q not found.", sid) },
		errExists:        func(sid string) string { return fmt.Sprintf("session %q already exists.", sid) },
		errGroupTaken:    func(g string) string { return fmt.Sprintf("group name %q is taken; choose another `name`.", g) },
		errMissingGroup:  `group-send: "group" is required.`,
		errMissingName:   `group-create: "name" is required. A session may create several groups, so each one needs a name.`,
		errSpawnNeedsGroup: func(groups []string) string {
			return fmt.Sprintf(`spawn: "group" is required. You coordinate: %s. Name the one to put the `+
				"worker in — even when there is only one.", strings.Join(groups, ", "))
		},
		errNotCoordinatorOf: func(g string) string {
			return fmt.Sprintf("spawn: you do not coordinate group %q, so you cannot put a worker in it.", g)
		},
		errBadAttachments: `group-send: "attachments" must be an array of file codes.`,
		errMissingAddress: `group-send: "to" must be an array of session names.`,
		errBadTo:          `group-send: "to" must be an array of session names, e.g. ["worker-1","worker-2"].`,
		errMissingText:    `group-send: "text" is required.`,
		errNoCoordinatedGroup: "spawn: you do not coordinate any group, so there is nowhere to put a worker. " +
			"Create your own with group-create first. Being merely a MEMBER of a group is not enough — " +
			"members cannot add sessions to it.",
		errNotMember:   func(g string) string { return fmt.Sprintf("group %q does not exist, or you are not in it.", g) },
		errNotMemberOf: func(to, g string) string { return fmt.Sprintf("%q is not a member of group %q.", to, g) },
		errPeerToPeer: func(g string) string {
			return fmt.Sprintf("group-send: members of group %q cannot message each other directly. Send to the "+
				"coordinator, which relays.", g)
		},
		errSelfAddress: "group-send: you cannot address yourself.",
	}
}

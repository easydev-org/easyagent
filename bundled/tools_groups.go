package bundled

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * Group tools: READ ONLY.
 *
 * A session can ask which groups it is in and who else is in one. It cannot create, join, leave or
 * edit a group. That is not a missing feature — a group is an organisational decision a person makes,
 * and a session that could assemble one would be choosing its own collaborators and then reading the
 * membership back. The console owns group creation; the agent side only ever looks.
 *
 * MEMBERSHIP IS THE ONLY PERMISSION, enforced by the store: a session asking about a group it is not
 * in gets an empty answer rather than an error naming the group, so the tool cannot be used to
 * enumerate the tenant.
 */

// groupListView is the display shape of one group row.
type groupListView struct {
	Name    string
	Members int
}

func (e *Extension) groupList(_ context.Context, tenant, session string, _ map[string]any) (agent.ToolOutcome, error) {
	groups, err := e.deps.GroupsOfSession(tenant, session)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if len(groups) == 0 {
		locale := e.localeOf(tenant, session)
		if strings.HasPrefix(locale, "zh") {
			return agent.ToolOutcome{Content: "本会话不属于任何群。"}, nil
		}
		return agent.ToolOutcome{Content: "This session belongs to no groups."}, nil
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%d group(s):\n", len(groups)))
	for _, g := range groups {
		b.WriteString(fmt.Sprintf("- %s (%d member(s))\n", g.Name, g.Members))
	}
	return agent.ToolOutcome{Content: strings.TrimRight(b.String(), "\n")}, nil
}

func (e *Extension) groupMembers(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	name := strings.TrimSpace(strArg(args, "group"))
	locale := e.localeOf(tenant, session)
	zh := strings.HasPrefix(locale, "zh")
	if name == "" {
		if zh {
			return agent.ToolOutcome{Content: "group 不能为空。"}, nil
		}
		return agent.ToolOutcome{Content: "group is required."}, nil
	}

	// THE MEMBERSHIP CHECK IS THE STORE'S, and an empty result is reported as "you are not in it"
	// rather than as an error naming the group. Confirming that a group exists to a non-member is
	// exactly what the API layer refuses to do, and the tool must not leak it the other way.
	if !e.deps.IsGroupMember(tenant, name, session) {
		if zh {
			return agent.ToolOutcome{Content: fmt.Sprintf("会话 %s 不属于群 %s。", session, name)}, nil
		}
		return agent.ToolOutcome{
			Content: fmt.Sprintf("Session %s is not a member of group %s.", session, name),
		}, nil
	}

	members, err := e.deps.GroupMemberProfiles(tenant, name)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if len(members) == 0 {
		if zh {
			return agent.ToolOutcome{Content: fmt.Sprintf("群 %s 没有成员。", name)}, nil
		}
		return agent.ToolOutcome{Content: fmt.Sprintf("Group %s has no members.", name)}, nil
	}

	/**
	 * A MEMBER LIST IS A LIST OF SESSIONS, NOT OF IDS.
	 *
	 * The ids alone answered "who is here" and nothing else: a coordinator could see three members but not
	 * which one it was, not what any of them does, and not who was answerable for the group — and it said so
	 * in its answer to a live group. The display name is what a person calls this session; the intro is the
	 * one line it wrote about itself. Those two are how a peer decides whom to address and with what, which is
	 * the entire use of this tool.
	 *
	 * THE ID STAYS: `group-send` addresses by id, because a display name is a label two sessions may share.
	 * It is written in parentheses after the name, where it is available without being the only thing shown.
	 *
	 * ROLE AND "THIS SESSION" ARE SEPARATE NOTES: the reader may be the coordinator, or an ordinary member
	 * reading its own row, and collapsing them would answer only one of the two questions.
	 */
	var b strings.Builder
	if zh {
		b.WriteString(fmt.Sprintf("群 %s 的 %d 个成员：\n", name, len(members)))
	} else {
		b.WriteString(fmt.Sprintf("%d member(s) of group %s:\n", len(members), name))
	}
	for _, m := range members {
		var notes []string
		if m.Role == GroupRoleCoordinator {
			if zh {
				notes = append(notes, "协调者")
			} else {
				notes = append(notes, "coordinator")
			}
		}
		if m.Session == session {
			if zh {
				notes = append(notes, "本会话")
			} else {
				notes = append(notes, "this session")
			}
		}

		// NAME (ID) — ROLE
		label := m.Session
		if m.Name != "" {
			label = fmt.Sprintf("%s (%s)", m.Name, m.Session)
		}
		if len(notes) > 0 {
			if zh {
				b.WriteString(fmt.Sprintf("- %s —— %s\n", label, strings.Join(notes, "、")))
			} else {
				b.WriteString(fmt.Sprintf("- %s — %s\n", label, strings.Join(notes, ", ")))
			}
		} else {
			b.WriteString("- " + label + "\n")
		}

		// THE INTRO IS INDENTED UNDER ITS OWNER: it describes that session, and a flat list of paragraphs
		// between the bullet points would read as if it belonged to whichever name happened to precede it.
		if m.Intro != "" {
			b.WriteString("  " + m.Intro + "\n")
		}
	}
	return agent.ToolOutcome{Content: strings.TrimRight(b.String(), "\n")}, nil
}

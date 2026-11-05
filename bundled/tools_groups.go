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

	members := e.deps.GroupMembers(tenant, name)
	if len(members) == 0 {
		if zh {
			return agent.ToolOutcome{Content: fmt.Sprintf("群 %s 没有成员。", name)}, nil
		}
		return agent.ToolOutcome{Content: fmt.Sprintf("Group %s has no members.", name)}, nil
	}

	var b strings.Builder
	if zh {
		b.WriteString(fmt.Sprintf("群 %s 的 %d 个成员：\n", name, len(members)))
	} else {
		b.WriteString(fmt.Sprintf("%d member(s) of group %s:\n", len(members), name))
	}
	for _, m := range members {
		if m == session {
			// MARKED, NOT OMITTED: "who else is here" includes knowing where YOU are, and a list that
			// silently drops the reader makes it hard to tell this session's own membership at a
			// glance.
			if zh {
				b.WriteString(fmt.Sprintf("- %s（本会话）\n", m))
			} else {
				b.WriteString(fmt.Sprintf("- %s (this session)\n", m))
			}
			continue
		}
		b.WriteString("- " + m + "\n")
	}
	return agent.ToolOutcome{Content: strings.TrimRight(b.String(), "\n")}, nil
}

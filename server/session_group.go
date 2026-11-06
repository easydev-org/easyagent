package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/easydev-org/easyagent/agentbus"
	"github.com/easydev-org/easyagent/store"
)

/**
 * A SESSION GETS A GROUP OF ITS OWN, so a person has somewhere to give it orders.
 *
 * THE TWO SCREENS ANSWER DIFFERENT QUESTIONS, and that is the whole reason this exists.
 *
 * A group's transcript is what a person WATCHES: plain conversation, no reasoning, no tool cards, no intermediate
 * files. It is the place where you see what the session is DOING — which is what supervision needs, because the
 * signal in "it is working on the failing test" is the sentence, not the sixteen tool calls that produced it.
 *
 * A session's transcript is what a person INSPECTS: every tool call, every argument, every result, every reasoning
 * step. You open it when the summary looks wrong, and it answers "what did it actually do".
 *
 * WITHOUT A GROUP the first screen did not exist. Sending an instruction meant opening the transcript, where the
 * instruction landed among tool cards and became indistinguishable in kind from what the agent said to itself. The
 * person supervising had to read the machinery to find the conversation.
 *
 * WHO GETS ONE:
 *
 *	user-created   yes  — this is the session a person will direct
 *	forked         yes  — a deliberate branch the user made, and it needs its own place to be directed
 *	spawned        NO   — a worker was created BY a coordinator to do a task. It is addressed through the group it
 *	                     was spawned into; giving every worker a group too would bury the list in rooms nobody ever
 *	                     speaks in, one per task.
 *
 * THE NAME IS THE SESSION'S LABEL, and it is not unique — two sessions called "check the build" get two groups
 * called that, which is correct: the label describes the work, and the id is what identifies the room.
 *
 * A FAILURE HERE DOES NOT FAIL THE SESSION. The session exists and works; it just has no room of its own yet. Losing
 * a session because a group could not be created would be the worse outcome, and re-running creation repairs it.
 */
func (s *Server) ensureSessionGroup(ctx context.Context, tenant, sessionID, sessionName string) (string, error) {
	if s.db == nil {
		return "", nil
	}
	locale := ""
	if row, ok, _ := s.db.GetSession(tenant, sessionID); ok {
		locale = row.Locale
	}
	name := strings.TrimSpace(sessionName)
	if name == "" {
		// A SESSION MAY HAVE NO LABEL — the console renders its id instead. The group needs SOMETHING a person can
		// read, so it takes the short form of the id rather than an empty title.
		name = shortIDOf(sessionID)
	}
	intro := sessionGroupIntro(locale, name)

	g := &store.Group{
		Name:      name,
		Intro:     intro,
		CreatedBy: "session:" + sessionID,
	}
	if err := s.db.CreateGroup(tenant, g, sessionID, nil, nowRFC3339()); err != nil {
		return "", err
	}
	if err := s.syncGroupSubscriptions(ctx, tenant, g.ID, nil); err != nil {
		// THE GROUP EXISTS AND IS CORRECT; only its routing is not established yet. Reported rather than rolled back:
		// deleting a room the user is about to speak in is worse than a routing delay they can retry.
		return g.ID, fmt.Errorf("group created but its subscriptions could not be written: %w", err)
	}
	s.notifyListChange(tenant, agentbus.SessionListChange{
		Kind: agentbus.ListKindGroup, Name: g.ID,
	})
	return g.ID, nil
}

// sessionGroupIntro is the one line that says what the group is FOR.
//
// IT IS WRITTEN IN THE SESSION'S LOCALE, because it is read by the person who will type into the group, and that
// person is reading everything else in that language.
func sessionGroupIntro(locale, name string) string {
	if strings.HasPrefix(locale, "zh") {
		return fmt.Sprintf("向 %s 下达指令的群：这里的发言会交给它处理。", name)
	}
	return fmt.Sprintf("Direct %s from here: what you post in this group is given to it.", name)
}

// shortIDOf returns the first 8 characters of an id, for a label that has to be readable.
func shortIDOf(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

/**
 * grantGroupPermissions gives a session every group capability, SCOPED TO ONE GROUP.
 *
 * THE SCOPE IS THE FEATURE. Before it, a session either held `group.spawn` or did not, so "this session may put
 * workers to work inside the group a person made for it" and "it may do that inside a group it made for itself" were
 * the same bit — and the second was strictly more authority than anyone granted.
 *
 * THE AUTO-CREATED GROUP IS THE CASE THE SCOPE EXISTS FOR: a session's own room is where a person will type
 * instructions, so the session may spawn workers there without a separate grant. A group some OTHER session created
 * does not carry this, and a coordinator that is merely a member of a group the user made carries nothing at all.
 *
 * THE GRANTS ARE IDEMPOTENT, because this runs on create and on fork and a client retry re-runs both.
 *
 * `grantedBy` records provenance: "auto:create:<session>" or "auto:fork:<parent>". A user looking at the permission
 * panel should be able to tell an automatic delegation from one they made.
 */
func (s *Server) grantGroupPermissions(tenant, sessionID, groupID, grantedBy string) error {
	if s.db == nil {
		return nil
	}
	scope := store.GroupScope(groupID)
	for _, cap := range []string{"group.create", "group.spawn", "group.send"} {
		if _, _, err := s.db.EnsureGrant(tenant, sessionID, store.SessionGrant{
			ExtID:      "bundled",
			Capability: cap,
			// `write`: none of these destroys anything, and all of them widen what the session can reach. Same tier
			// the manifest declares for the same capabilities, so the panel does not describe one grant two ways.
			Risk:      "write",
			Scope:     scope,
			GrantedBy: grantedBy,
		}); err != nil {
			return err
		}
	}
	return nil
}

/**
 * setupSessionGroup is the whole "a new session gets its own room" step, in one call.
 *
 * IT IS ONE FUNCTION BECAUSE THE TWO HALVES MUST AGREE. A group without the scoped grants is a room the session
 * cannot delegate from; grants pointing at a group that was never created name nothing. Creating the group and then
 * granting for it keeps the id in hand, and a caller cannot accidentally do one without the other.
 *
 * IT DOES NOT REPORT A FAILED GRANT AS A FAILED CREATION. The group and the session are both real and usable; the
 * missing grant shows up as a refusal the next time the session tries to spawn, which is a message a person can act
 * on. Turning it into "your session could not be created" would be a worse description of what happened.
 */
func (s *Server) setupSessionGroup(ctx context.Context, tenant, sessionID, sessionName, grantedBy string) string {
	groupID, err := s.ensureSessionGroup(ctx, tenant, sessionID, sessionName)
	if err != nil || groupID == "" {
		return groupID
	}
	_ = s.grantGroupPermissions(tenant, sessionID, groupID, grantedBy)
	return groupID
}

/**
 * deleteSessionGroups removes the groups a session coordinated by itself, along with their transcripts.
 *
 * ONLY THE AUTO-CREATED ONES, identified by `created_by`. A group a person made and happened to put this session in
 * charge of is the person's, not the session's; deleting a session must not take their room with it, and its
 * coordinator slot is repaired by the same membership cleanup that removes the session everywhere else.
 *
 * BEST-EFFORT. The session row is already gone by the time this runs, and a lingering group is a row a person can
 * delete; failing here would report an error for a deletion that succeeded.
 */
func (s *Server) deleteSessionGroups(ctx context.Context, tenant, sessionID string) {
	if s.db == nil {
		return
	}
	owned, err := s.db.GroupsCreatedBy(tenant, "session:"+sessionID)
	if err != nil {
		return
	}
	for _, g := range owned {
		previous, _ := s.db.SessionsInGroupIDs(tenant, g.ID, "")
		if err := s.db.DeleteGroup(tenant, g.ID); err != nil {
			continue
		}
		_ = s.db.DeleteGroupMessages(tenant, g.ID)
		if s.channels != nil {
			_ = s.channels.DropGroup(ctx, tenant, g.ID, previous)
		}
		s.notifyListChange(tenant, agentbus.SessionListChange{
			Kind: agentbus.ListKindGroup, Name: g.ID, Deleted: true,
		})
	}
}

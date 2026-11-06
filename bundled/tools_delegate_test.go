package bundled

import (
	"context"
	"strings"
	"testing"
)

/**
 * THE DELEGATION CHAIN, TESTED END TO END AT THE TOOL LAYER.
 *
 * Four behaviours make the chain work, and each has been wrong at least once:
 *
 *   · a session coordinates AT MOST ONE group, and asking twice answers rather than fails;
 *   · a spawned worker LOSES `group.create`, which is what keeps delegation one level deep;
 *   · the worker joins the caller's group as a plain member;
 *   · the worker CAN report back, while a member still cannot message another member.
 */
type chainStub struct {
	*stubDeps
	// coordinated maps session -> the group it coordinates.
	coordinated map[string]string
	// membership maps group -> members.
	membership map[string][]string
	creates    []string    // group names passed to CreateGroupForSession
	failed     bool        // make CreateGroupForSession fail
	forked     []string    // worker names forked
	added      [][2]string // {group, session} pairs added
	revoked    [][2]string // {session, capability} pairs revoked
	published  [][3]string // {group, to, text}
	posted     [][2]string // {group, text} — everything that reached the transcript
	// postedAttachments records the file codes each post carried, in call order.
	postedAttachments [][]string
	forkExclude       []string
	forkErr           error
}

func newChainStub() *chainStub {
	return &chainStub{
		stubDeps: &stubDeps{existing: map[string]bool{}}, coordinated: map[string]string{},
		membership: map[string][]string{},
	}
}

func (c *chainStub) CoordinatedGroup(_, session string) (string, error) {
	return c.coordinated[session], nil
}

func (c *chainStub) CoordinatedGroupOrMembership(_, session string) string {
	if g := c.coordinated[session]; g != "" {
		return g
	}
	for g, list := range c.membership {
		for _, m := range list {
			if m == session {
				return g
			}
		}
	}
	return ""
}

func (c *chainStub) CreateGroupForSession(_, group, coordinator string) error {
	if c.failed {
		return errGroupExistsStub
	}
	c.creates = append(c.creates, group)
	c.coordinated[coordinator] = group
	c.membership[group] = []string{coordinator}
	return nil
}

func (c *chainStub) ForkSession(_, _, name string, exclude []string) (string, error) {
	if c.forkErr != nil {
		return "", c.forkErr
	}
	// THE ID IS WHAT THE CALLER GETS BACK, and it is distinct from the label on purpose — these tests
	// exist partly to catch a caller that confuses the two.
	id := "id-" + name
	c.forked = append(c.forked, id)
	c.forkExclude = exclude
	c.existing[id] = true
	return id, nil
}

func (c *chainStub) AddGroupMemberOnly(_, group, session string) error {
	c.added = append(c.added, [2]string{group, session})
	c.membership[group] = append(c.membership[group], session)
	return nil
}

func (c *chainStub) GroupCoordinator(_, group string) (string, error) {
	for s, g := range c.coordinated {
		if g == group {
			return s, nil
		}
	}
	return "", nil
}

func (c *chainStub) IsGroupMember(_, group, session string) bool {
	for _, m := range c.membership[group] {
		if m == session {
			return true
		}
	}
	return false
}

func (c *chainStub) PostGroupMessage(_, group, from string, to []string, text string, attachments []string) error {
	c.postedAttachments = append(c.postedAttachments, attachments)
	c.posted = append(c.posted, [2]string{group, text})
	for _, rcpt := range to {
		c.published = append(c.published, [3]string{group, rcpt, text})
	}
	return nil
}

var errGroupExistsStub = errString("group already exists")

type errString string

func (e errString) Error() string { return string(e) }

func newChainExt(t *testing.T, c *chainStub) *Extension {
	t.Helper()
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

/**
 * A SECOND CALL IS AN ANSWER, NOT A FAILURE.
 *
 * A model that lost track of whether it had created a group asks again; replying "you already
 * coordinate X" gives it the name it needs to keep working. An error would leave it with nothing to
 * act on and invite it to try a different name — which is exactly the second group the rule forbids.
 */
func TestGroupCreateIsIdempotentAndAnswers(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	out, err := e.groupCreate(ctx, "t", "alice", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.creates) != 1 || c.creates[0] != "alice" {
		t.Fatalf("creates = %v, want one group named after the session", c.creates)
	}
	if !strings.Contains(out.Content, "alice") {
		t.Errorf("created message should name the group:\n%s", out.Content)
	}

	// Second call: no new group, and the name comes back.
	out, err = e.groupCreate(ctx, "t", "alice", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.creates) != 1 {
		t.Errorf("a second call created another group: %v", c.creates)
	}
	if !strings.Contains(out.Content, "already") {
		t.Errorf("second call should say the group already exists:\n%s", out.Content)
	}
	meta, _ := out.Metadata.(map[string]any)
	if got := meta["group"]; got != "alice" {
		t.Errorf("metadata group = %v, want alice", got)
	}
}

// A group name that is already taken is reported with a hint to choose another, because the derived
// name (the session name) can collide with a group a person made.
func TestGroupCreateReportsATakenName(t *testing.T) {
	c := newChainStub()
	c.failed = true
	e := newChainExt(t, c)

	out, err := e.groupCreate(context.Background(), "t", "alice", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(out.Content), "taken") {
		t.Errorf("want a taken-name answer, got %q", out.Content)
	}
}

/**
 * THE WORKER LOSES `group.create`, AND THE MASK IS A REVOCATION.
 *
 * This is the property that bounds the delegation depth. It is enforced by removing the grant rather
 * than by a flag on the session, so the same permission gate that guards every privileged call also
 * guards this one — a separate "is a worker" bit would be the first thing to be forgotten.
 */
func TestSpawnMasksGroupCreateOnTheWorker(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	// alice coordinates a group.
	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}

	out, err := e.spawn(ctx, "t", "alice", map[string]any{"task": "count the files"})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.forked) != 1 {
		t.Fatalf("forked = %v, want exactly one worker", c.forked)
	}
	worker := c.forked[0]

	/**
	 * THE MASK IS APPLIED DURING THE COPY, NOT AFTERWARDS.
	 *
	 * Both group grants must be in the exclusion list, because a fork inherits everything the parent
	 * has: a worker of a session that could spawn could also spawn, and the depth would be whatever the
	 * model felt like. Revoking after the fork would leave a window where the grant is held.
	 */
	want := map[string]bool{"bundled/group.create": false, "bundled/group.spawn": false}
	masked := true
	for _, k := range c.forkExclude {
		if _, ok := want[k]; ok {
			want[k] = true
		}
	}
	for k, got := range want {
		if !got {
			masked = false
			t.Errorf("the fork did not exclude %s (got %v)", k, c.forkExclude)
		}
	}
	if !masked {
		t.Errorf("the fork did not exclude group.create: %v", c.forkExclude)
	}

	// And it was added to alice's group as a plain member.
	if len(c.added) != 1 || c.added[0] != [2]string{"alice", worker} {
		t.Errorf("added = %v, want the worker in alice's group", c.added)
	}

	// The task went out as a group message addressed to the worker.
	if len(c.published) != 1 {
		t.Fatalf("published = %v, want one task message", c.published)
	}
	if c.published[0][0] != "alice" || c.published[0][1] != worker {
		t.Errorf("task published to %v, want group/worker", c.published[0])
	}
	if !strings.Contains(c.published[0][2], "count the files") {
		t.Errorf("the task text is missing from the brief:\n%s", c.published[0][2])
	}
	// The worker is told it cannot create groups, or it will try and be refused.
	if !strings.Contains(c.published[0][2], "cannot create groups") {
		t.Errorf("the brief must tell the worker it cannot create groups:\n%s", c.published[0][2])
	}
	_ = out
}

/**
 * A FAILED FORK MEANS NO WORKER AT ALL.
 *
 * The exclusions are part of the fork now, so a fork that cannot apply them does not happen — there is
 * no half-created worker holding grants it was meant to lose. This asserts that nothing downstream ran:
 * no membership, no task, no report to wait for.
 */
func TestSpawnLeavesNothingBehindWhenTheForkFails(t *testing.T) {
	c := newChainStub()
	c.forkErr = errString("store unavailable")
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.spawn(ctx, "t", "alice", map[string]any{"task": "x"}); err == nil {
		t.Fatal("spawn must fail when the fork cannot be created")
	}
	if len(c.added) != 0 || len(c.published) != 0 {
		t.Errorf("a failed fork still added or published: added=%v published=%v", c.added, c.published)
	}
}

// A session in no group has nowhere to put a worker; it is told to create a group rather than given a
// bare failure.
func TestSpawnRequiresAGroup(t *testing.T) {
	c := newChainStub()
	c.existing["alice"] = true
	e := newChainExt(t, c)

	out, err := e.spawn(context.Background(), "t", "alice", map[string]any{"task": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "group") {
		t.Errorf("want an instruction to create a group, got %q", out.Content)
	}
	if len(c.forked) != 0 {
		t.Errorf("a worker was forked despite having no group: %v", c.forked)
	}
}

/**
 * THE WORKER CAN REPORT BACK — the direction that was refused before.
 *
 * `group-send` once required the caller to be the coordinator, so a worker's report was rejected every
 * time and every delegation ended in silence. The rule is now about the TARGET: up to the coordinator and
 * down to a member are both allowed, sideways is not.
 */
func TestGroupSendAllowsBothDirections(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.spawn(ctx, "t", "alice", map[string]any{"task": "work"}); err != nil {
		t.Fatal(err)
	}
	worker := c.forked[0]

	// Downward: coordinator → member.
	if _, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{worker}, "text": "keep going",
	}); err != nil {
		t.Fatal(err)
	}

	// Upward: member → coordinator. THIS IS THE ONE THAT USED TO FAIL.
	out, err := e.groupSend(ctx, "t", worker, map[string]any{
		"group": "alice", "to": []any{"alice"}, "text": "done",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(out.Content), "not") {
		t.Errorf("the worker's report was refused: %q", out.Content)
	}
}

// Sideways is still refused: two members talking would make the coordinator optional, and it is the one
// thing that knows what it handed out.
func TestGroupSendRefusesMemberToMember(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1", "w2"}

	out, err := e.groupSend(ctx, "t", "w1", map[string]any{
		"group": "alice", "to": []any{"w2"}, "text": "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "coordinator") {
		t.Errorf("want a member-to-member refusal, got %q", out.Content)
	}
	if len(c.published) != 0 {
		t.Errorf("a refused message was still published: %v", c.published)
	}
}

// And nobody outside the group can be reached at all.
func TestGroupSendRefusesOutsiders(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	out, err := e.groupSend(ctx, "t", "mallory", map[string]any{
		"group": "alice", "to": []any{"alice"}, "text": "let me in",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "not in it") {
		t.Errorf("an outsider must be refused without confirming the group exists, got %q", out.Content)
	}
}

// The tool must not be usable to talk to itself — a loop the model would not notice.
func TestGroupSendRefusesSelfAddress(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	out, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"alice"}, "text": "note to self",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "yourself") {
		t.Errorf("want a self-address refusal, got %q", out.Content)
	}
}

/**
 * EXACTLY TWO TOOLS ARE PRIVILEGED, AND THEY NEED DIFFERENT GRANTS.
 *
 * "May this session assemble a group" and "may it put other sessions to work" are separate decisions:
 * a session can legitimately have either without the other, so neither grant may imply the other. And
 * every OTHER tool must stay unprivileged — a worker loses both group grants, and if `group-send` or
 * `group-members` required one it could not report back, which would make every delegation a dead end.
 */
func TestOnlyGroupToolsRequireGrants(t *testing.T) {
	m, err := ParseManifest()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"group-create": "group.create",
		"spawn":        "group.spawn",
	}
	for _, tl := range m.Tools {
		capability, privileged := want[tl.Name]
		if !privileged {
			if len(tl.Requires) > 0 {
				t.Errorf("%q requires %v; a spawned worker loses both group grants and must still be "+
					"able to work and report back", tl.Name, tl.Requires)
			}
			continue
		}
		if len(tl.Requires) != 1 || tl.Requires[0] != capability {
			t.Errorf("%q requires %v, want [%s]", tl.Name, tl.Requires, capability)
		}
	}
	// Both grants must be declared, or requiring them is a dead end nobody can authorise.
	declared := map[string]bool{}
	for _, p := range m.Permissions {
		declared[p.Key] = true
	}
	for _, capability := range want {
		if !declared[capability] {
			t.Errorf("%q is required by a tool but not declared as a grantable permission", capability)
		}
	}
}

/**
 * THE SAME MESSAGE, TO SEVERAL MEMBERS, REACHES EXACTLY THOSE MEMBERS.
 *
 * `to` is an array because "hand this to three workers" is ONE decision. Three separate calls would be
 * three chances to write three slightly different texts, and the recipients would disagree about what
 * they were told. But it is still not a broadcast: the set of people who hear it is exactly the set the
 * caller named — each name is published to its own topic.
 */
func TestGroupSendToSeveralMembers(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1", "w2", "w3"}

	out, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"w1", "w2"}, "text": "same instruction",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 2 {
		t.Fatalf("published %d messages, want 2 (one per named member): %v", len(c.published), c.published)
	}
	// ONE TOPIC PER RECIPIENT — this is what keeps w3 from hearing a message it was not named in.
	got := map[string]string{}
	for _, p := range c.published {
		got[p[1]] = p[2]
	}
	if _, ok := got["w1"]; !ok {
		t.Errorf("w1 did not receive the message: %v", got)
	}
	if _, ok := got["w2"]; !ok {
		t.Errorf("w2 did not receive the message: %v", got)
	}
	if _, ok := got["w3"]; ok {
		t.Error("w3 received a message it was not named in")
	}
	// Every recipient gets the SAME text — that is the point of one call.
	for _, text := range got {
		if text != "same instruction" {
			t.Errorf("text = %q, want the same body for everyone", text)
		}
	}
	if !strings.Contains(out.Content, "w1") || !strings.Contains(out.Content, "w2") {
		t.Errorf("the answer should name who it reached:\n%s", out.Content)
	}
}

/**
 * THE WHOLE LIST IS VALIDATED BEFORE ANYTHING IS SENT.
 *
 * A half-delivered instruction is worse than a refused one: some workers start on a task the others
 * never received, and nothing in the transcript says which. So one bad name refuses the call instead of
 * reaching the others first.
 */
func TestGroupSendRejectsTheWholeListOnOneBadName(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1"}

	out, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"w1", "not-a-member"}, "text": "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 0 {
		t.Errorf("a partially-addressed message was delivered to %d recipient(s)", len(c.published))
	}
	if !strings.Contains(out.Content, "not-a-member") {
		t.Errorf("the refusal should name the bad recipient:\n%s", out.Content)
	}
}

// A bare string is refused rather than wrapped: accepting both shapes would let a caller use a scalar
// and never learn the field is an array.
func TestGroupSendRequiresAnArray(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1"}

	out, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": "w1", "text": "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "array") {
		t.Errorf("a scalar `to` should be refused with a shape hint, got %q", out.Content)
	}
	if len(c.published) != 0 {
		t.Error("a malformed `to` still published")
	}
}

// Duplicates collapse and empty entries drop: the same session named twice is a mistake, not an
// instruction to send the same text twice.
func TestGroupSendCollapsesDuplicates(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1"}

	if _, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"w1", "w1", "", "  "}, "text": "x",
	}); err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 1 {
		t.Errorf("published %d times for a duplicated name, want 1", len(c.published))
	}
}

/**
 * A MEMBER MAY ADDRESS THE COORDINATOR, AND ONLY THE COORDINATOR — even in a list.
 *
 * The list form must not become a way around the sideways rule: a member sending to [coordinator, peer]
 * is refused whole, because the alternative is delivering the half that happens to be allowed.
 */
func TestGroupSendMemberCanOnlyAddressTheCoordinator(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1", "w2"}

	// w1 → coordinator: allowed.
	if _, err := e.groupSend(ctx, "t", "w1", map[string]any{
		"group": "alice", "to": []any{"alice"}, "text": "done",
	}); err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 1 {
		t.Fatalf("the report to the coordinator was not sent: %v", c.published)
	}

	// w1 → [coordinator, w2]: refused whole.
	c.published = nil
	out, err := e.groupSend(ctx, "t", "w1", map[string]any{
		"group": "alice", "to": []any{"alice", "w2"}, "text": "done",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 0 {
		t.Errorf("a peer in the list did not stop the whole send: %v", c.published)
	}
	if !strings.Contains(out.Content, "coordinator") {
		t.Errorf("want a sideways refusal, got %q", out.Content)
	}
}

/**
 * NO `to` MEANS A REPLY TO THE GROUP — THE MOST COMMON CALL, NOT A MISTAKE.
 *
 * A coordinator's answer to what a user asked IN the group belongs in that group. Leaving it in the
 * coordinator's own conversation means the person who asked never sees it, which reads to them as being
 * ignored — and that is exactly what a live group showed: the question, and no answer at all.
 *
 * NOTHING IS DELIVERED TO MEMBERS. A reply is for the group's reader, not an instruction; waking three
 * workers to read "收到，我来汇总" would cost three turns and produce three more replies.
 */
func TestGroupSendWithoutToIsAReply(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1", "w2"}

	out, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "text": "收到，我来汇总",
	})
	if err != nil {
		t.Fatal(err)
	}
	// It reached the transcript...
	if len(c.posted) != 1 {
		t.Fatalf("posted %d time(s), want 1: %v", len(c.posted), c.posted)
	}
	// ...and nobody.
	if len(c.published) != 0 {
		t.Errorf("a reply was delivered to %v; a reply is for the group's reader", c.published)
	}
	if !strings.Contains(out.Content, "群") && !strings.Contains(out.Content, "group") {
		t.Errorf("the answer should say where it went: %q", out.Content)
	}
	// The metadata has no `to` — there was none to report.
	if _, hasTo := out.Metadata.(map[string]any)["to"]; hasTo {
		t.Error("a reply reported a recipient list")
	}
}

// With `to`, the message is an instruction: delivered to exactly those members.
func TestGroupSendWithToIsAnInstruction(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1", "w2"}

	if _, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"w1"}, "text": "you do it",
	}); err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 1 || c.published[0][1] != "w1" {
		t.Errorf("delivered to %v, want exactly w1", c.published)
	}
}

// A group name is still required: the tool cannot decide where to post without one.
func TestGroupSendRequiresAGroup(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	out, err := e.groupSend(context.Background(), "t", "alice", map[string]any{"text": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "group") {
		t.Errorf("want a missing-group message, got %q", out.Content)
	}
}

/**
 * A COORDINATOR CAN HAND A GROUP A FILE.
 *
 * A tool that produced a picture, a report or a recording returns a `file:<code>`; handing that code to the group
 * is the whole operation, because the bytes already live in the tenant's blob store. Without this the only way
 * to share a result was to describe it in words — which is the one thing a picture cannot survive.
 */
func TestGroupSendCarriesAttachments(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1"}

	if _, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"w1"},
		"text": "here is the chart", "attachments": []any{"file-1", "file-2"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(c.postedAttachments) != 1 {
		t.Fatalf("posted %d message(s), want 1", len(c.postedAttachments))
	}
	got := c.postedAttachments[0]
	if len(got) != 2 || got[0] != "file-1" || got[1] != "file-2" {
		t.Errorf("attachments = %v, want [file-1 file-2]", got)
	}
}

/**
 * A PICTURE WITH NO CAPTION IS A MESSAGE.
 *
 * `text` used to be mandatory, so sending a file alone required inventing a caption — and the caption people
 * invent ("here you go") says less than the picture does.
 *
 * A MESSAGE WITH NEITHER IS STILL REFUSED: that one is not a message, it is a mistake.
 */
func TestGroupSendAllowsAnAttachmentWithoutText(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1"}

	// Attachment only: accepted.
	if _, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "attachments": []any{"file-1"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(c.postedAttachments) != 1 || len(c.postedAttachments[0]) != 1 {
		t.Fatalf("the attachment-only message did not reach the group: %v", c.postedAttachments)
	}

	// Neither: refused, and nothing was posted.
	before := len(c.posted)
	out, err := e.groupSend(ctx, "t", "alice", map[string]any{"group": "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.posted) != before {
		t.Error("an empty message was posted")
	}
	if !strings.Contains(out.Content, "text") && !strings.Contains(out.Content, "内容") {
		t.Errorf("the refusal does not say what is missing: %q", out.Content)
	}
}

// A malformed `attachments` value is refused rather than silently dropped.
func TestGroupSendRejectsNonArrayAttachments(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	c.existing["alice"] = true
	if _, err := e.groupCreate(context.Background(), "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice"}
	out, err := e.groupSend(context.Background(), "t", "alice", map[string]any{
		"group": "alice", "text": "x", "attachments": "file-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "attachments") {
		t.Errorf("want a complaint about attachments, got %q", out.Content)
	}
}

/**
 * ADDRESSING SOMEONE WHO IS NOT IN THE GROUP IS REFUSED, AND NOTHING IS SENT.
 *
 * THE FAILURE THIS PREVENTS IS SILENT AND EXPENSIVE: a message delivered to a session that is not a member would
 * put group traffic in a private conversation, where the recipient has no context for it and no way to answer into
 * the group. The transcript would also record a delivery that the group never really made.
 *
 * IT IS REFUSED IN THE TOOL, not by the channel layer. The channel routes by membership patterns, so an outsider
 * simply matches nothing — the message would be stored and reach nobody, and the coordinator would believe it had
 * been delivered. Checking here is what turns "silently unheard" into a message the model can act on.
 */
func TestGroupSendRefusesARecipientOutsideTheGroup(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	c.existing["mallory"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	// The group holds alice and w1; mallory exists as a session but is NOT a member.
	c.membership["alice"] = []string{"alice", "w1"}

	out, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"mallory"}, "text": "you are not in this group",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "mallory") {
		t.Errorf("the refusal must name the recipient, got %q", out.Content)
	}
	// NOTHING REACHED THE TRANSCRIPT: a refusal that still posted would be worse than no check at all.
	if len(c.posted) != 0 || len(c.published) != 0 {
		t.Errorf("a refused message was posted: posted=%v published=%v", c.posted, c.published)
	}
}

/**
 * A REFUSED LIST IS REFUSED WHOLE.
 *
 * Half-delivering an instruction is worse than refusing it: some workers would start on a task the others never
 * received, and nothing in the transcript says which. The valid name must not slip through because it was checked
 * before the invalid one.
 */
func TestGroupSendWithOneOutsiderDeliversToNobody(t *testing.T) {
	c := newChainStub()
	e := newChainExt(t, c)
	ctx := context.Background()

	c.existing["alice"] = true
	c.existing["mallory"] = true
	if _, err := e.groupCreate(ctx, "t", "alice", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	c.membership["alice"] = []string{"alice", "w1"}

	if _, err := e.groupSend(ctx, "t", "alice", map[string]any{
		"group": "alice", "to": []any{"w1", "mallory"}, "text": "half of you can do this",
	}); err != nil {
		t.Fatal(err)
	}
	if len(c.published) != 0 {
		t.Errorf("a partially valid list delivered to %v; the list is refused whole", c.published)
	}
}

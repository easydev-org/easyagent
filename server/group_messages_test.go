package server

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/agentbus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * A GROUP HAS ITS OWN TRANSCRIPT, AND A MESSAGE IS STORED BEFORE ANYONE HEARS IT.
 *
 * The order is the contract: a recipient that receives a message can always find it. Delivering first would
 * allow a message to be heard and never recorded — the one failure that cannot be repaired, because there
 * is nothing to point at.
 */
func TestGroupMessageIsStoredThenDelivered(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	member := newSession(t, c, "worker")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{member}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// A USER posts: it reaches the coordinator and lands in the transcript.
	res, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "please split this",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Message == nil || res.Message.Seq != 1 {
		t.Fatalf("stored message = %+v, want seq 1", res.Message)
	}
	// AN EMPTY SENDER MEANS THE USER, which is a real distinction the console renders.
	if res.Message.Sender != "" {
		t.Errorf("sender = %q, want empty for a user post", res.Message.Sender)
	}
	if len(res.DeliveredTo) != 1 || res.DeliveredTo[0] != coord {
		t.Errorf("delivered to %v, want the coordinator only", res.DeliveredTo)
	}

	// AND IT IS READABLE afterwards — the whole point of a transcript.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 1 || list.Messages[0].Body != "please split this" {
		t.Fatalf("transcript = %+v", list.Messages)
	}
	if list.LatestSeq != 1 {
		t.Errorf("latest_seq = %d, want 1", list.LatestSeq)
	}
}

// Sequence numbers are assigned per group and strictly increase, so a watch can resume from the newest one
// it saw without gaps.
func TestGroupMessageSeqIncreases(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		res, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
			GroupId: groupIDByName(t, c, "g"), Text: "m", Sender: coord,
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Message.Seq != int64(i) {
			t.Fatalf("message %d got seq %d", i, res.Message.Seq)
		}
	}

	// The cursor is EXCLUSIVE: after 2 means "3 onward".
	page, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g"), AfterSeq: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || page.Messages[0].Seq != 3 {
		t.Fatalf("after_seq=2 returned %+v", page.Messages)
	}
}

/**
 * A NON-MEMBER CANNOT POST, AND CANNOT READ.
 *
 * Both answers are NOT_FOUND rather than PERMISSION_DENIED: the difference would let any session discover
 * which groups a tenant has by watching which names fail differently.
 */
func TestGroupMessagesRequireMembership(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	outsider := newSession(t, c, "mallory")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	_, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "hi", Sender: outsider,
	})
	if err == nil {
		t.Fatal("a non-member posted to the group")
	}
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", got)
	}

	_, err = c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g"), Session: outsider})
	if err == nil {
		t.Fatal("a non-member read the transcript")
	}
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", got)
	}

	// And the member CAN.
	if _, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g"), Session: coord}); err != nil {
		t.Fatalf("the coordinator could not read its own group: %v", err)
	}
}

// Deleting a group deletes its transcript: the rows would otherwise be unreachable and a later group with
// the same name would inherit a stranger's conversation.
func TestDeleteGroupRemovesTranscript(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{GroupId: groupIDByName(t, c, "g"), Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteGroup(ctx, &agentv1.DeleteGroupRequest{Id: groupIDByName(t, c, "g")}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 0 {
		t.Errorf("the recreated group inherited %d old message(s)", len(list.Messages))
	}
	if list.LatestSeq != 0 {
		t.Errorf("latest_seq = %d, want 0", list.LatestSeq)
	}
}

/**
 * A GROUP ALWAYS HAS A COORDINATOR, AND THAT IS ENFORCED WHERE THE STATE WOULD BE CREATED.
 *
 * The store refuses to remove the last coordinator ("move the role first"), so the "routes nowhere" case
 * cannot be reached through the API — which is the right place to prevent it. This test records that
 * guarantee rather than simulating an unreachable state: a handler for it would be code no deployment can
 * exercise, and the next reader would assume it protects something.
 */
func TestCoordinatorCannotBeLeftAbsent(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RemoveGroupMember("acme", "g", coord); err == nil {
		t.Fatal("the store allowed the last coordinator to be removed")
	}
	// And the group still posts, because nothing was changed.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{GroupId: groupIDByName(t, c, "g"), Text: "x"}); err != nil {
		t.Fatalf("the group stopped accepting messages: %v", err)
	}
}

/**
 * ONE RECIPIENT, ONE RULE: THE COORDINATOR.
 *
 * A user writes to the group and a member reports to it; both arrive at the same place. There is no `to` field
 * any more, because a field a caller could set and the server would ignore is worse than no field: the caller
 * believes it addressed someone, and the failure looks like delivery rather than like a missing capability.
 */
func TestGroupMessageRecipientIsAlwaysTheCoordinator(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	w1 := newSession(t, c, "w1")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{w1}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// A USER'S MESSAGE reaches the coordinator.
	res, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{GroupId: groupIDByName(t, c, "g"), Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 1 || res.DeliveredTo[0] != coord {
		t.Errorf("a user's message went to %v, want the coordinator", res.DeliveredTo)
	}

	// A MEMBER'S REPORT reaches the same place.
	res, err = c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "done", Sender: w1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 1 || res.DeliveredTo[0] != coord {
		t.Errorf("a member's report went to %v, want the coordinator", res.DeliveredTo)
	}

	// A COORDINATOR'S POST IS RECORDED AND WAKES NOBODY: it hands out work through its own tool, which stores
	// and delivers that message itself. This RPC is not that path.
	res, err = c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "a note", Sender: coord,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 0 {
		t.Errorf("a coordinator's post reached %v, want nobody", res.DeliveredTo)
	}

	// ALL THREE ARE IN THE TRANSCRIPT, which is what makes the record complete regardless of who was woken.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 3 {
		t.Fatalf("transcript holds %d messages, want 3", len(list.Messages))
	}
}

/**
 * A NON-MEMBER CANNOT POST, SO A MESSAGE CANNOT COME FROM OUTSIDE.
 *
 * The `to` targets are gone with the field, so the remaining validation is on the SENDER: a session that is not
 * in the group gets NOT_FOUND, which is the same answer as a group that does not exist.
 */
func TestGroupPostRefusesANonMemberSender(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	outsider := newSession(t, c, "mallory")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "x", Sender: outsider,
	}); err == nil {
		t.Fatal("a non-member posted to the group")
	}

	// NOTHING WAS STORED, so no recipient can find a message from outside.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 0 {
		t.Errorf("a refused message was still stored: %+v", list.Messages)
	}
}

/**
 * A PICTURE WITH NO CAPTION IS A MESSAGE.
 *
 * An attachment is a `file:<code>` reference — the same shape a session message uses — so the text may be
 * empty. Requiring words would make the sender invent a caption to send a photo, and the natural workaround
 * ("here is a picture") adds nothing a reader needs.
 *
 * EMPTINESS IS REFUSED ONLY WHEN BOTH ARE EMPTY: a message with neither is not a message.
 */
func TestGroupMessageNeedsTextOrAnAttachment(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// Neither: refused.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{GroupId: groupIDByName(t, c, "g")}); err == nil {
		t.Fatal("a message with no text and no attachment was accepted")
	}

	// Attachment only: accepted, and the reference survives the round trip.
	sent, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId:     groupIDByName(t, c, "g"),
		Attachments: []*agentv1.FileRef{{Code: "file-1", Name: "shot.png", Size: 1234}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sent.Message.Body != "" {
		t.Errorf("body = %q, want empty", sent.Message.Body)
	}
	if len(sent.Message.Attachments) != 1 || sent.Message.Attachments[0].Code != "file-1" {
		t.Fatalf("attachments = %+v", sent.Message.Attachments)
	}
	if sent.Message.Attachments[0].Name != "shot.png" || sent.Message.Attachments[0].Size != 1234 {
		t.Errorf("the label or size was lost: %+v", sent.Message.Attachments[0])
	}

	// And it is readable later — the reference is part of the transcript, not just the acknowledgement.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 1 || len(list.Messages[0].Attachments) != 1 {
		t.Fatalf("transcript = %+v", list.Messages)
	}
}

/**
 * A REFERENCE WITH NO CODE NAMES NOTHING, SO IT IS DROPPED.
 *
 * Storing it would put a tile in the transcript that every member sees and nothing can load — and the reader
 * would have no way to tell whether the sender or the reader was at fault.
 */
func TestGroupMessageDropsAnEmptyAttachmentRef(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// An empty ref alongside a real one: the real one survives, the empty one is gone.
	sent, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "look",
		Attachments: []*agentv1.FileRef{{Code: "", Name: "broken"}, {Code: "file-2", Name: "ok"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent.Message.Attachments) != 1 || sent.Message.Attachments[0].Code != "file-2" {
		t.Fatalf("attachments = %+v, want only file-2", sent.Message.Attachments)
	}
}

/**
 * AN ATTACHMENT CARRIES THE TYPE THE AGENT DERIVED, NOT WHAT THE SENDER CLAIMED.
 *
 * A renderer has to decide between an image, a player and a file chip before it can draw anything. With no
 * type on the reference it answered "file" for everything, so a picture posted to a group showed as a generic
 * document — while the blob store had known it was `image/png` all along.
 *
 * THE CALLER CANNOT CHOOSE THE TYPE, and that is the point: the bytes were sniffed at ingest, and the record
 * that describes them is the only authority. A `FileRef` on input carries a code; name, size and type are
 * filled from the stored blob.
 */
func TestGroupMessageAttachmentTypeComesFromTheBlob(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// A blob the agent ingested, so its type was derived from the bytes.
	const code = "blob-1"
	if _, err := db.UpsertFile(store.AgentFile{
		Code: code, Tenant: "acme", SHA256: "sha-blob-1",
		Name: "shot.png", Mime: "image/png", Size: 2048,
	}); err != nil {
		t.Fatal(err)
	}

	// The caller sends ONLY a code, and a misleading name for good measure.
	sent, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId:     groupIDByName(t, c, "g"),
		Attachments: []*agentv1.FileRef{{Code: code, Name: "not-a-picture.bin"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent.Message.Attachments) != 1 {
		t.Fatalf("attachments = %+v", sent.Message.Attachments)
	}
	a := sent.Message.Attachments[0]
	if a.Mime != "image/png" {
		t.Errorf("mime = %q, want the type derived at ingest", a.Mime)
	}
	if a.Size != 2048 {
		t.Errorf("size = %d, want it filled from the blob", a.Size)
	}
	// A NAME THE SENDER GAVE IS KEPT: it is a label, and the sender may have a better one than the file's.
	if a.Name != "not-a-picture.bin" {
		t.Errorf("name = %q, the sender's label was overwritten", a.Name)
	}

	// AND IT SURVIVES A RELOAD, because the type is stored with the message rather than re-derived per reader.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 1 || list.Messages[0].Attachments[0].Mime != "image/png" {
		t.Fatalf("reloaded attachment lost its type: %+v", list.Messages)
	}
}

/**
 * A SESSION MESSAGE'S ATTACHMENT KEEPS ITS TYPE TOO — the same defect, on the older path.
 *
 * `mailbox.FileRef` has always had a `Mime` field and nothing ever set it, so an attachment folded into a
 * session's history as an untyped file and the console drew a generic chip instead of a picture. Nothing
 * noticed because the deployed tenant had never posted one.
 *
 * BOTH PATHS NOW USE fillFileRef, so this pins the rule once for both.
 */
func TestFileRefIsFilledFromTheBlob(t *testing.T) {
	_, db, _ := newTestServerWith(t, nil)
	if _, err := db.UpsertFile(store.AgentFile{
		Code: "blob-2", Tenant: "acme", SHA256: "sha-blob-2",
		Name: "voice.wav", Mime: "audio/wav", Size: 4096,
	}); err != nil {
		t.Fatal(err)
	}

	// Only a code: everything else is filled from the blob.
	got := fillFileRef(db, "acme", &agentv1.FileRef{Code: "blob-2"})
	if got.GetMime() != "audio/wav" {
		t.Errorf("mime = %q, want audio/wav from the blob", got.GetMime())
	}
	if got.GetSize() != 4096 {
		t.Errorf("size = %d, want it filled from the blob", got.GetSize())
	}
	if got.GetName() != "voice.wav" {
		t.Errorf("name = %q, want it filled from the blob when the caller gave none", got.GetName())
	}

	// A NAME THE CALLER GAVE IS KEPT: it is a label, and the sender may have a better one.
	named := fillFileRef(db, "acme", &agentv1.FileRef{Code: "blob-2", Name: "my note.wav"})
	if named.GetName() != "my note.wav" {
		t.Errorf("name = %q, the caller's label was overwritten", named.GetName())
	}

	// AN UNKNOWN BLOB CHANGES NOTHING: the reference is still an honest record of what was attached.
	unknown := fillFileRef(db, "acme", &agentv1.FileRef{Code: "nope", Name: "x"})
	if unknown.GetMime() != "" || unknown.GetName() != "x" {
		t.Errorf("an unknown blob was guessed at: %+v", unknown)
	}
}

/**
 * A THUMBNAIL THAT IS REFERENCED MUST EXIST.
 *
 * `thumb_code` pointed at `<code>-thumb` while the row recording that thumbnail was written with a digest of
 * `sha + ":thumb"` — 70 characters into a 64-character column. The row was therefore never stored under the
 * digest the insert expected, and the console asked for a thumbnail that did not exist: every image loaded and
 * its preview 404'd.
 *
 * THE THUMBNAIL IS ITS OWN BLOB WITH ITS OWN BYTES, so it is hashed from those bytes. That is both correct and
 * the only value that fits.
 */
func TestThumbnailRowExistsWhenThumbCodeIsSet(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	// A PNG LARGER THAN 512px, because the probe only produces a thumbnail for an image it has to downscale —
	// an image already inside the bound is its own thumbnail, so there is nothing to store and nothing to
	// check.
	var buf bytes.Buffer
	canvas := image.NewRGBA(image.Rect(0, 0, 900, 900))
	for y := 0; y < 900; y++ {
		for x := 0; x < 900; x++ {
			canvas.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}
	if err := png.Encode(&buf, canvas); err != nil {
		t.Fatal(err)
	}
	img := buf.Bytes()
	res, err := c.IngestFile(ctx, &agentv1.IngestFileRequest{Data: img, Name: "shot.png"})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := c.GetFileMeta(ctx, &agentv1.GetFileMetaRequest{Code: res.Code})
	if err != nil {
		t.Fatal(err)
	}
	if meta.ThumbCode == nil || *meta.ThumbCode == "" {
		t.Fatal("no thumbnail was produced, so nothing was checked")
	}

	// THE POINT: the code it points at resolves.
	if _, err := c.GetFileMeta(ctx, &agentv1.GetFileMetaRequest{Code: *meta.ThumbCode}); err != nil {
		t.Fatalf("thumb_code %q points at nothing: %v", *meta.ThumbCode, err)
	}

	// AND IT SURVIVES A RE-UPLOAD OF THE SAME BYTES. The second upload reuses the parent row (identical digest),
	// so the thumbnail row already exists — an insert that insists on creating it again fails on the primary
	// key, and a caller that reads that failure as "not stored" leaves the parent pointing at nothing.
	again, err := c.IngestFile(ctx, &agentv1.IngestFileRequest{Data: img, Name: "shot.png"})
	if err != nil {
		t.Fatal(err)
	}
	if again.Code != res.Code {
		t.Fatalf("the second upload minted a new code %q; this test needs deduplication", again.Code)
	}
	meta2, err := c.GetFileMeta(ctx, &agentv1.GetFileMetaRequest{Code: res.Code})
	if err != nil {
		t.Fatal(err)
	}
	if meta2.ThumbCode == nil || *meta2.ThumbCode == "" {
		t.Fatal("the re-upload lost its thumbnail reference")
	}
	if _, err := c.GetFileMeta(ctx, &agentv1.GetFileMetaRequest{Code: *meta2.ThumbCode}); err != nil {
		t.Fatalf("after a re-upload, thumb_code %q points at nothing: %v", *meta2.ThumbCode, err)
	}
}

/**
 * A GROUP HAS AN INTRO, AND IT REACHES THE PLACES THAT NEED IT.
 *
 * The intro is the one line saying what the group is FOR — the same idea as a session's intro, and the reason
 * a woken coordinator does not have to infer its job from the names of the sessions it is writing to. That
 * inference is exactly what produced a coordinator answering a question that was meant for the group.
 *
 * IT IS EDITABLE AFTER CREATION, with the same absent/empty rule the picture uses: absent = leave it alone,
 * present-but-empty = clear it.
 */
func TestGroupIntroRoundTrips(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
		Intro: "release coordination for acme/app",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Group.Intro != "release coordination for acme/app" {
		t.Errorf("intro = %q, want what was set at creation", created.Group.Intro)
	}

	// IT IS READABLE FROM THE GROUP, which is what the info page shows.
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if got.Group.Intro != "release coordination for acme/app" {
		t.Errorf("read back intro = %q", got.Group.Intro)
	}

	// LEAVING IT ALONE: a picture-only save must not wipe the intro.
	pic := "file:abc"
	if _, err := c.UpdateGroup(ctx, &agentv1.UpdateGroupRequest{Id: groupIDByName(t, c, "g"), AvatarFile: &pic}); err != nil {
		t.Fatal(err)
	}
	got, _ = c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if got.Group.Intro != "release coordination for acme/app" {
		t.Errorf("a picture-only save changed the intro to %q", got.Group.Intro)
	}

	// CLEARING IT: present-but-empty means "say nothing".
	empty := ""
	if _, err := c.UpdateGroup(ctx, &agentv1.UpdateGroupRequest{Id: groupIDByName(t, c, "g"), Intro: &empty}); err != nil {
		t.Fatal(err)
	}
	got, _ = c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if got.Group.Intro != "" {
		t.Errorf("intro = %q after clearing it", got.Group.Intro)
	}
	// AND THE PICTURE SURVIVED that save, for the same reason.
	if got.Group.AvatarFile != pic {
		t.Errorf("an intro-only save changed the picture to %q", got.Group.AvatarFile)
	}
}

/**
 * A GROUP ROW SAYS WHAT WAS LAST SAID.
 *
 * A session row has always shown the newest message's words and when; a group row showed a member count and
 * nothing else, so scanning the list told a reader how many people were in a group but never what they were
 * talking about — the one piece of information a list is for.
 *
 * THE NEWEST MESSAGE THAT HAS WORDS WINS. A group whose most recent post is a picture would otherwise render an
 * empty preview, which reads as "nothing was said" rather than "something was sent".
 */
func TestListGroupsCarriesTheTranscriptPreview(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	/**
	 * AN EMPTY GROUP HAS NO PREVIEW, and that must not be an error.
	 *
	 * THE LIST ALSO CONTAINS THE COORDINATOR'S OWN ROOM now, which is empty for the same reason. The lookup is by
	 * id, because that is what identifies the group under test — a name would match the room too, since a session and
	 * its room share a label.
	 */
	gid := groupIDByName(t, c, "g")
	list, err := c.ListGroups(ctx, &agentv1.ListGroupsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var empty *agentv1.Group
	for _, g := range list.Groups {
		if g.Id == gid {
			empty = g
		}
	}
	if empty == nil {
		t.Fatalf("the group is missing from the list: %+v", list.Groups)
	}
	if empty.LastMessagePreview != "" {
		t.Fatalf("an empty group has preview %q", empty.LastMessagePreview)
	}

	// A message with words becomes the preview, with its timestamp.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "please check main",
	}); err != nil {
		t.Fatal(err)
	}
	list, err = c.ListGroups(ctx, &agentv1.ListGroupsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var g *agentv1.Group
	for _, row := range list.Groups {
		if row.Id == gid {
			g = row
		}
	}
	if g == nil {
		t.Fatalf("the group disappeared from the list: %+v", list.Groups)
	}
	if g.LastMessagePreview != "please check main" {
		t.Errorf("preview = %q, want the newest message", g.LastMessagePreview)
	}
	if g.LastMessageAt == "" {
		t.Error("the preview has no timestamp")
	}

	// AN ATTACHMENT-ONLY MESSAGE DOES NOT BLANK THE PREVIEW: the words before it still say what is going on.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Attachments: []*agentv1.FileRef{{Code: "x"}},
	}); err != nil {
		t.Fatal(err)
	}
	list, _ = c.ListGroups(ctx, &agentv1.ListGroupsRequest{})
	for _, row := range list.Groups {
		if row.Id != gid {
			continue
		}
		if row.LastMessagePreview != "please check main" {
			t.Errorf("preview = %q after an attachment-only post, want the words before it",
				row.LastMessagePreview)
		}
	}
}

/**
 * A LIST CHANGE CROSSES REPLICAS.
 *
 * A nudge used to be published straight into one process's subscriber map, so a row written on replica A never
 * reached a client connected to replica B. The symptom is not an error: the preview, the unread count and the
 * ordering simply stay stale for as long as the page is open.
 *
 * THE NUDGE NAMES THE ROW AND NOTHING ELSE. The watcher re-reads it from the database, so a duplicated or
 * out-of-order notification is harmless — applying it twice sends the same row twice, and the database always
 * has the last word.
 */
func TestListChangeIsPublishedForEveryReplica(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	fake := &recordingListChanges{}
	srv.SetListChanges(fake)

	coord := newSession(t, c, "coord")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	// THE NUDGE CARRIES THE ID, because that is what the watcher re-reads the row with — a name would make the
	// notification a second place that assumes a label identifies something.
	gid := created.Group.Id

	// A new group is a new row.
	if !fake.saw("group", gid, false) {
		t.Errorf("creating a group published %+v, want a group change", fake.all())
	}

	// A message changes the row's preview and timestamp.
	fake.reset()
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: gid, Text: "hello",
	}); err != nil {
		t.Fatal(err)
	}
	if !fake.saw("group", gid, false) {
		t.Errorf("a message published %+v, want a group change", fake.all())
	}

	// Deleting it removes the row.
	fake.reset()
	if _, err := c.DeleteGroup(ctx, &agentv1.DeleteGroupRequest{Id: gid}); err != nil {
		t.Fatal(err)
	}
	if !fake.saw("group", gid, true) {
		t.Errorf("deleting a group published %+v, want a removal", fake.all())
	}
}

// recordingListChanges is a ListChangeBus that keeps what it was told.
type recordingListChanges struct {
	mu   sync.Mutex
	seen []agentbus.SessionListChange
}

func (r *recordingListChanges) PublishListChange(_ context.Context, tenant string, c agentbus.SessionListChange) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c.Tenant = tenant
	r.seen = append(r.seen, c)
	return nil
}

func (r *recordingListChanges) SubscribeListChanges(context.Context, string) (<-chan agentbus.SessionListChange, func(), error) {
	// The test only checks what this replica PUBLISHES; a subscription would need a second replica.
	ch := make(chan agentbus.SessionListChange)
	return ch, func() { close(ch) }, nil
}

func (r *recordingListChanges) saw(kind, name string, deleted bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.seen {
		if c.Kind == kind && c.Name == name && c.Deleted == deleted {
			return true
		}
	}
	return false
}

func (r *recordingListChanges) all() []agentbus.SessionListChange {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]agentbus.SessionListChange(nil), r.seen...)
}

func (r *recordingListChanges) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = nil
}

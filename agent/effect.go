package agent

/**
 * Effect is the IMPACT of calling a tool, independent of who may call it.
 *
 * IT IS A SEPARATE AXIS FROM `Requires`. `Requires` answers "does this session hold the permission";
 * Effect answers "what happens once it does". A session may be fully authorised and still deserve a
 * confirmation before merging a pull request, because authorisation and consequence are different
 * questions.
 *
 * FOUR LEVELS, DEFINED BY WHOSE STATE THE CALL TOUCHES AND WHAT REMAINS AFTERWARDS:
 *
 *	none    Nothing changes. Reading a repository file, searching history, fetching a page.
 *	low     Only the caller's OWN state changes, and nothing anyone else depends on. Opening a pull
 *	        request, closing your own mistaken one, generating an image, starting a CI run.
 *	medium  State that OTHERS ALREADY DEPEND ON changes. Closing someone else's pull request, merging
 *	        to a shared branch, restarting a deployment people are using. It can often be undone, but
 *	        "undone" is not "never happened": someone has already pulled, deployed or planned around it.
 *	high    An asset is CONSUMED. Deleting a repository, dropping a database, force-pushing over
 *	        history. There is no new state to point at and nothing to revert to.
 *
 * THE LINE BETWEEN low AND medium IS WHOSE, NOT WHAT. Both change state. What separates them is whether
 * anyone other than the caller was relying on the thing that changed — which is why closing your own
 * pull request and closing someone else's land in different levels while looking like the same verb.
 * "Can I revert it?" is the wrong test: merging and deleting are both hard to revert, but only one of
 * them leaves something to revert TO.
 *
 * THE LINE BETWEEN medium AND high IS WHAT REMAINS. A medium change leaves the system in a new, valid
 * state. A high act leaves a hole.
 *
 * "RUNS ARBITRARY CODE" IS ALWAYS high. A tool whose blast radius is decided by its arguments — a shell
 * command, a browser click, a script — cannot declare a narrower effect, because the narrowness would be
 * a property of the call rather than of the tool.
 */
type Effect string

const (
	// EffectNone changes nothing.
	EffectNone Effect = "none"
	// EffectLow changes only the caller's own state.
	EffectLow Effect = "low"
	// EffectMedium changes state others already depend on.
	EffectMedium Effect = "medium"
	// EffectHigh consumes an asset, leaving nothing to revert to.
	EffectHigh Effect = "high"
)

// Valid reports whether e is one of the four declared effects.
func (e Effect) Valid() bool {
	switch e {
	case EffectNone, EffectLow, EffectMedium, EffectHigh:
		return true
	}
	return false
}

/**
 * Rank orders the effects from least to most consequential, for the one rule that compares them: a
 * permission's `risk` may not understate the effect of a tool it gates.
 */
func (e Effect) Rank() int {
	switch e {
	case EffectNone:
		return 0
	case EffectLow:
		return 1
	case EffectMedium:
		return 2
	case EffectHigh:
		return 3
	}
	return -1
}

/**
 * NeedsConfirmation reports whether a call should be confirmed before it runs.
 *
 * ONLY THE TWO HIGHEST DO. `low` touches only the caller's own state, and asking about every such act
 * would train the user to click through the dialog — which is how a confirmation stops meaning anything.
 */
func (e Effect) NeedsConfirmation() bool {
	return e == EffectMedium || e == EffectHigh
}

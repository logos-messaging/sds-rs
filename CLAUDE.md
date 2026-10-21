# Code Style Guide

Write code a teammate can read once and understand. Short, plain, obvious.

## Core Rules

1. **Clarity beats cleverness.** If it needs explaining, rewrite it.
2. **Small functions.** One job each. Aim for under 20 lines; split past 40.
3. **Names carry meaning.** Good names replace most comments.
4. **Match the codebase.** Follow existing patterns, naming, and structure before your own preferences.
5. **Do only what was asked.** No speculative features, abstractions, or "just in case" code.

## Naming

- Functions are verbs: `loadUser`, `sendInvoice`, `isExpired`.
- Booleans read as questions: `isValid`, `hasAccess`, `shouldRetry`.
- Variables are nouns that say what they hold: `activeUsers`, not `data` or `list2`.
- No abbreviations unless universal (`id`, `url`, `db`).
- Length matches scope: `i` in a 3-line loop is fine; module-level names should be descriptive.

## Functions

- One level of abstraction per function.
- Max ~3 parameters. More than that → pass an object.
- Return early instead of nesting:

```ts
// Avoid
function process(order) {
  if (order) {
    if (order.isPaid) {
      ship(order);
    }
  }
}

// Prefer
function process(order) {
  if (!order?.isPaid) return;
  ship(order);
}
```

- No flag arguments that switch behavior (`save(user, true)`). Write two functions.
- Keep side effects obvious from the name.

## Comments

**Default: no comment.** Make the code explain itself first.

Write a comment only when it says **why**, never **what**:

```py
# Avoid
# Increment retry count by one
retries += 1

# Prefer (explains a non-obvious reason)
# Stripe rate-limits bursts; back off before the 4th call
time.sleep(backoff)
```

- One line is the norm. Two is the limit for most comments.
- No comments that restate the function name, parameters, or types.
- No commented-out code. Delete it; git remembers.
- No banner comments, section dividers, or changelogs in code.
- Docstrings only for public APIs, and keep them to a sentence or two.
- TODOs need a reason: `# TODO: remove once v2 API ships`.

## Structure

- Max nesting depth of 2–3. Extract or return early beyond that.
- Group related code together; keep files focused on one concern.
- Prefer flat data and plain functions over deep class hierarchies.
- Don't create a helper, wrapper, or abstraction for something used once.
- Delete dead code, unused imports, and unused variables.

## Simplicity

- Use the standard library and existing utilities before writing new ones.
- Avoid premature optimization and premature generalization.
- Avoid magic numbers; name them: `MAX_RETRIES = 3`.
- Prefer explicit over implicit. No hidden control flow.
- One way to do a thing per codebase.

## Error Handling

- Fail fast with clear messages: `throw new Error("User 42 not found")`.
- Handle errors where you can act on them; otherwise let them propagate.
- Never swallow errors silently.
- Don't add defensive checks for states that can't happen.

## Formatting

- Use the project's formatter and linter. Don't hand-format against them.
- Blank lines separate ideas; don't use them as decoration.
- Keep lines readable (~100 chars).

## Before You Finish

- [ ] Could a new teammate understand each function in one read?
- [ ] Is every comment explaining *why*, not *what*?
- [ ] Any function doing more than one thing?
- [ ] Any code that wasn't asked for?
- [ ] Any dead code, debug logs, or leftover TODOs?
- [ ] Does it match the surrounding code's style?

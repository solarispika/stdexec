# Injecting a scheduler into a sequence sender's env

A pattern shared by these example wrappers:

- `examples/fsevents_wrapper.hpp`
- `examples/da_wrapper.hpp`
- `examples/inotify_wrapper.hpp`
- `examples/velx_wrapper.hpp`
- `examples/rdc_pool_wrapper.hpp`

Each wrapper exposes a `sequence_sender_t` whose underlying callback /
IO completion needs to land on a caller-selected execution context
(`libdispatch_queue` on macOS, `io_uring_context` on Linux,
`windows_thread_pool` on Windows). The wrapper's `subscribe` is
constrained at compile time to require the matching scheduler type in
the receiver env (via `exec::__env_has_scheduler`), so the caller has
to surface that scheduler somehow.

The natural stdexec spelling — `stdexec::starts_on(sched, child)` or
`stdexec::write_env(child, env_with_get_scheduler=sched)` — does not
work today.

## Why not `stdexec::starts_on` / `stdexec::write_env`?

`starts_on(sched, child)` rewrites the child via the regular-sender
path: roughly

```cpp
__sequence(continues_on(stdexec::just(), sched), child)
```

For a regular sender child this is fine — the `__sequence` adaptor
sequences a "schedule onto sched" sender then the child, completion
travels through normally. But `__sequence(regular, sequence_sender)` is
itself **not** a sequence sender: it has no `subscribe` customization,
no `item_types`, and `enable_sequence_sender` is false. When
downstream's `transform_each` looks for `item_types` on the wrapper, it
collapses the per-item type to `set_value_t()` and the wrapper's value
channel disappears.

The user-visible symptom: `transform_each(then([](Batch){...}))`
downstream gets the lambda invoked with `()` instead of `(Batch)`, and
compiles fail at the `then` site complaining the function isn't
callable with the given arguments.

`stdexec::write_env(child, env)` has the same problem from a different
angle — `__write_env_impl` inherits `__sexpr_defaults` and provides no
sequence-sender machinery, so the wrapper is regular-sender-shaped
regardless of what the child was.

Both symptoms are tracked in
[`docs/plans/2026-04-29-stdexec-write_env-sequence-sender-issue.md`](../docs/plans/2026-04-29-stdexec-write_env-sequence-sender-issue.md).

## The workaround: `exec::sequence_with_scheduler`

`include/exec/on_scheduler.hpp` ships a tiny shared adapter (~80 LoC)
that does just the env-injection part, preserving sequence-sender
attributes. Spelled at the use site:

```cpp
exec::sequence_with_scheduler(sched, ctx.watch())
| exec::transform_each(stdexec::then([](Batch b){ ... }))
| exec::ignore_all_values()
```

It does **not** reschedule — it only writes `get_scheduler -> sched`
into the receiver env so the wrapped sequence sender's `subscribe`
constraint is satisfied. The wrapper itself decides where its work
runs (libdispatch source on the queue, ThreadpoolEnvironment bound to
the pool, io_uring SQE submission, etc).

Sketch of the adapter:

```cpp
template <class _Snd, class _Sched>
struct __on_scheduler_sender {
  using sender_concept        = sequence_sender_tag;
  using item_types            = __item_types_of_t<_Snd>;          // forwarded
  using completion_signatures = stdexec::__completion_signatures_of_t<_Snd>;

  _Snd   __snd_;
  _Sched __sched_;

  template <stdexec::receiver _Rcvr>
  auto subscribe(_Rcvr __rcvr) && {
    return exec::subscribe(static_cast<_Snd&&>(__snd_),
                           __on_scheduler_rcvr<_Rcvr, _Sched>{std::move(__rcvr),
                                                              std::move(__sched_)});
  }
};
```

The wrapper-side receiver returns a joined env exposing
`get_scheduler -> _Sched` so the inner sender's `subscribe` (which has
a `requires` clause demanding the scheduler is in the env) is
satisfied. All four completion CPOs (`set_next` / `set_value` /
`set_error` / `set_stopped`) just forward.

One subtle gotcha hit while writing this: `stdexec::prop{stdexec::get_scheduler, sched}`
returns its value as `_Sched const&`, which fails any
`same_as<..., _Sched>` constraint downstream. The adapter uses a small
custom env fragment (`__sched_prop`) whose `query` returns by value.

## What the wrappers require of the receiver env

The corresponding `subscribe` on the wrapper is constrained at compile
time to require the matching scheduler type, via the shared
`exec::__env_has_scheduler` concept:

```cpp
template <stdexec::receiver _Rcvr>
  requires exec::__env_has_scheduler<stdexec::env_of_t<_Rcvr>,
                                     /* libdispatch_scheduler /
                                        io_uring_scheduler /
                                        windows_thread_pool::scheduler */>
auto subscribe(_Rcvr __rcvr) const -> __op<_Rcvr>;
```

This catches at compile time the case where a caller composes the
wrapper with an unrelated scheduler — without the constraint the
wrapper would silently fall back to a default queue / pool and the
user-supplied scheduler would have no effect.

## When this can be deleted

Once stdexec gains sequence-sender-aware `write_env` (mechanical fix
per the upstream issue), `exec::sequence_with_scheduler` collapses to
a thin alias over `stdexec::write_env(snd, env_with_get_scheduler=sched)`,
and the explanation here can be reduced to "use `write_env`". Until
then the shared adapter carries the ~80 LoC of mechanical glue for all
five example wrappers.

**Title**: `write_env` and `starts_on` don't preserve `sequence_sender` semantics

I was building an FSEvents wrapper as a `sequence_sender_tag` and wanted the consumer side to express "run this on libdispatch queue X" with a stdexec idiom. `starts_on` was the obvious first try, then `write_env` after I noticed `starts_on` does scheduling on top of env injection. Both fail in the same way for the same reason. Filing in case it's useful.

Tested against `main` @ `8c5eedd0`.

## The bug

When the child of `stdexec::write_env(child, env)` is a sequence sender, the resulting wrapper isn't. `__write_env_impl` inherits `__sexpr_defaults` and provides only `__get_attrs` / `__get_env` / `__get_completion_signatures` — no `subscribe`, no `item_types`, `enable_sequence_sender = false`. So `exec::subscribe`, finding no `subscribe` customization and `enable_sequence_sender = false` but `__next_connectable` true (the receiver is a sequence receiver), drops into the "sender as sequence of one" branch: `set_next(rcvr, wrapper)` is invoked exactly once and per-item type information collapses.

`stdexec::starts_on(sched, child)` fails the same way through a different route: it rewrites to `__sequence(continues_on(just(), sched), child)`, and `__sequence(regular, sequence)` is also not a sequence sender.

The user-visible symptom: `transform_each(then([](int){...}))` downstream gets the lambda invoked with `()` instead of `(int)` because the wrapper's value channel collapsed to `set_value_t()`.

## Repro

```cpp
#include "exec/sequence/transform_each.hpp"
#include "exec/sequence/ignore_all_values.hpp"
#include "exec/sequence_senders.hpp"
#include "exec/static_thread_pool.hpp"
#include "stdexec/execution.hpp"

struct my_seq_sender
{
    using sender_concept        = exec::sequence_sender_tag;
    using __item_t              = decltype(stdexec::just(int{}));
    using item_types            = exec::item_types<__item_t>;
    using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t(),
                                                                 stdexec::set_stopped_t()>;

    template <class Rcvr>
    struct op
    {
        using operation_state_concept = stdexec::operation_state_t;
        Rcvr rcvr_;
        void start() noexcept
        {
            auto next = exec::set_next(rcvr_, stdexec::just(42));
            (void) next;
            stdexec::set_value(static_cast<Rcvr&&>(rcvr_));
        }
    };

    template <stdexec::receiver Rcvr>
    auto subscribe(Rcvr r) const -> op<Rcvr> { return {std::move(r)}; }
};

// Baseline (compiles): plain sequence_sender → transform_each.
auto baseline()
{
    return my_seq_sender{}
         | exec::transform_each(stdexec::then([](int){ /* fine */ }));
}

// Bug A (fails to compile): write_env between sequence_sender and transform_each.
auto bug_write_env()
{
    return stdexec::write_env(my_seq_sender{}, stdexec::env<>{})
         | exec::transform_each(stdexec::then([](int){ /* never sees int */ }));
}

// Bug B (fails to compile when forced through sync_wait): starts_on.
void bug_starts_on()
{
    exec::static_thread_pool pool{1};
    stdexec::sync_wait(stdexec::starts_on(pool.get_scheduler(), my_seq_sender{})
                     | exec::transform_each(stdexec::then([](int){ /* never sees int */ }))
                     | exec::ignore_all_values());
}
```

Build:

```
clang++ -std=c++20 -I/path/to/stdexec/include -c repro.cpp
```

Errors I see (clang 16, C++20):

- **`bug_write_env`**: fails at `__then.hpp:88` — `deduced type '__sexpr_t<then_t, lambda, __sexpr<just()>>' does not satisfy '__well_formed_sender'`. The trace runs through `transform_each`'s `get_item_types`, which deduced a single `just()` item from the write_env wrapper rather than the original `just(int)`.
- **`bug_starts_on`**: fails inside `__sender_concepts.hpp:159` with the readable diagnostic `_FUNCTION_IS_NOT_CALLABLE_WITH_THE_GIVEN_ARGUMENTS_, _IN_ALGORITHM_ then_t, _WITH_FUNCTION_ (lambda taking int), _WITH_ARGUMENTS_ ()` — i.e. the lambda was asked to handle `()` instead of `(int)`.

## What I think the fix is, and where it gets hard

`write_env` is the easy half. It doesn't change scheduling, only augments the env the inner subscribe sees. Sequence-sender-aware version is mechanical: detect `enable_sequence_sender<child_t>`, pick a different `__sexpr_impl` specialization that declares `sender_concept = sequence_sender_tag`, forwards `item_types` and the appropriate completion signatures, and provides a `subscribe` that wraps the receiver with a `__sched_env`-style joined env before forwarding to child. The four completion CPOs (set_next, set_value, set_error, set_stopped) just pass through. Same `write_env(snd, env)` call, no API change for users.

`starts_on` is the harder half. The strict analogy with regular `starts_on` ("start on sched, then become snd"; completion happens wherever the inner sender puts it) is mostly useless for sequences — for a push-driven sequence like FSEvents, items come from the source's own thread regardless of where `subscribe()` ran. So the defensible default is "all items run on sched". But that still leaves a real implementation choice with different cost profiles:

- **Push**: rewrite as `write_env(seq, env_with_get_scheduler=sched) | transform_each(continues_on(_, sched))` (plus wrapping the outer sequence completion). Strong guarantee: every item completes on sched even if the source ignores the env. Cost: an extra hop per item even when the source is already on sched.
- **Pull**: stop at `write_env(seq, env_with_get_scheduler=sched)` and rely on the source to read `get_scheduler` and dispatch accordingly. Zero overhead when honored, no effect when ignored.

For my use case (FSEvents) pull is right and free — the wrapper's receiver dispatches each item via libdispatch from the env-supplied scheduler. Other sources may want push. I'd suggest fixing `write_env` first (that gives users pull explicitly) and treating `starts_on` over a sequence as a separate decision once there's consensus on which guarantee is canonical.

## Workaround in user code

About 80 lines of custom adapter. Sketch:

```cpp
template <class Snd, class Sched>
struct on_queue_sender {
    using sender_concept = exec::sequence_sender_tag;
    using item_types = exec::__item_types_of_t<Snd>;
    using completion_signatures = stdexec::__completion_signatures_of_t<Snd>;

    template <stdexec::receiver R>
    auto subscribe(R r) && {
        return exec::subscribe(std::move(snd_),
                               wrap_rcvr<R, Sched>{std::move(r), sched_});
    }
    Snd snd_; Sched sched_;
};
```

`wrap_rcvr::get_env()` returns a joined env exposing `get_scheduler -> Sched`. One thing that bit me: `stdexec::prop{stdexec::get_scheduler, sched}` returns `_Value const&` from `query`, which fails any `same_as<...>` constraint in the wrapped sender's `subscribe` template. I had to write a small env fragment that returns by value. Worth knowing if anyone else hits this.

<details>
<summary>Full standalone adapter (~85 lines)</summary>

```cpp
// on_queue: env-injection adapter that exposes a scheduler via
// get_scheduler in the receiver env. Needed because stdexec::starts_on
// rewrites a sequence_sender child via the regular-sender path
// (__sequence(continues_on(just(), sched), child)) and loses item_types.

template <class _Sched>
struct __sched_prop
{
  _Sched __sched_;

  [[nodiscard]]
  constexpr auto query(stdexec::get_scheduler_t) const noexcept -> _Sched
  {
    return __sched_;
  }
};

template <class _Rcvr, class _Sched>
struct __on_queue_rcvr
{
  using receiver_concept = stdexec::receiver_tag;

  _Rcvr  __rcvr_;
  _Sched __sched_;

  [[nodiscard]]
  auto get_env() const noexcept
  {
    return stdexec::env{__sched_prop<_Sched>{__sched_}, stdexec::get_env(__rcvr_)};
  }

  template <class _Item>
  auto set_next(_Item&& __item) -> exec::next_sender_of_t<_Rcvr, _Item>
  {
    return exec::set_next(__rcvr_, static_cast<_Item&&>(__item));
  }

  void set_value() noexcept
  {
    stdexec::set_value(static_cast<_Rcvr&&>(__rcvr_));
  }

  void set_stopped() noexcept
  {
    stdexec::set_stopped(static_cast<_Rcvr&&>(__rcvr_));
  }

  template <class _E>
  void set_error(_E&& __e) noexcept
  {
    stdexec::set_error(static_cast<_Rcvr&&>(__rcvr_), static_cast<_E&&>(__e));
  }
};

template <class _Snd, class _Sched>
struct __on_queue_sender
{
  using sender_concept        = exec::sequence_sender_tag;
  using item_types            = exec::__item_types_of_t<_Snd>;
  using completion_signatures = stdexec::__completion_signatures_of_t<_Snd>;

  _Snd   __snd_;
  _Sched __sched_;

  template <stdexec::receiver _Rcvr>
  auto
  subscribe(_Rcvr __rcvr) && -> exec::subscribe_result_t<_Snd, __on_queue_rcvr<_Rcvr, _Sched>>
  {
    return exec::subscribe(static_cast<_Snd&&>(__snd_),
                           __on_queue_rcvr<_Rcvr, _Sched>{std::move(__rcvr),
                                                          std::move(__sched_)});
  }
};

struct __on_queue_t
{
  template <stdexec::scheduler _Sched, class _Snd>
  auto operator()(_Sched __sched, _Snd __snd) const -> __on_queue_sender<_Snd, _Sched>
  {
    return {std::move(__snd), std::move(__sched)};
  }
};

inline constexpr __on_queue_t on_queue{};
```

</details>

## Environment

stdexec @ `main` `8c5eedd0` (verified against this exact commit).
clang 16, C++20, macOS arm64. Pure template-instantiation issue; reproduces independent of platform.

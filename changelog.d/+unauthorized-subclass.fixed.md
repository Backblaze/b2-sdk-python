`EmailNotVerified` and `NoPaymentHistory` are no longer turned into a plain `Unauthorized` by the session, so uploads fail right away instead of being retried.

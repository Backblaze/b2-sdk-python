Applies to documentation in this repository, including the Sphinx tree in `doc/`, and to Python docstrings.

# Writing style

Use this guide for new and edited documentation and docstrings. Do not apply it to
`CHANGELOG.md`, `changelog.d/` fragments, or dated records. These rules are inspired by
ASD-STE100 and are written for this project; they are not the standard.

## Rules

1. Put one idea in each sentence.
2. Prefer active voice and name the person or component performing the action.
3. Write procedure steps as commands, with one action per step.
4. Use one term for each meaning. Follow the term table below.
5. Choose simple verbs such as use, send, start, and stop.
6. Keep articles such as “a”, “an”, and “the” where they make a sentence clear.
7. Use plain words and remove intensifiers that add no meaning.
8. Repeat a noun when a pronoun could refer to more than one thing.
9. Use “must” for a requirement, “can” for an ability, and “should” for a recommendation.
10. Put the condition before the action.
11. Give exact numbers and units when they are known. Do not invent them.
12. Keep paragraphs short. Use a list for a sequence of steps.
13. Keep identifiers, flags, error strings, code, and URLs unchanged.
14. Change wording only; preserve the meaning of the original statement.

## Terms

Use the names in the public [`b2sdk.v3` exports](../b2sdk/v3/__init__.py) when referring to an API identifier.

| Concept | Preferred wording |
| --- | --- |
| Storage container | Use “bucket”; the SDK exports `Bucket`. |
| Stored data | Use “file” for a named file and “file version” for a specific version; the SDK exports `FileVersion`. |
| Application credential | Use “application key” for an account credential; the SDK exports `ApplicationKey`. |
| Authorization | Use “authorization token” for a token used to authorize requests; distinguish it from an application key. |
| Customer-managed encryption | Use “SSE-C key” for the customer-managed encryption key; the SDK exports `EncryptionKey`. |
| Transfers | Use “upload” for sending data to B2 and “download” for receiving it from B2; `Bucket` has upload and download methods. |
| Data units | Use “part” for a large-file part, represented by `Part`; use “chunk” for a buffer or a piece of a stream. |
| Repeated requests | Use “attempt” for each try, including the first; use “retry” for an additional try after a failure. |

## Check meaning before editing

- Preserve numbers, units, and limits.
- Preserve whether a statement is a requirement, recommendation, or ability: “must”, “should”, and “can” are not interchangeable.
- Preserve security wording and credential requirements.
- Preserve compatibility, deprecation, and removal statements.
- Check claims against the code or the relevant contract. Flag a statement that cannot be verified; do not guess.

## Examples from this repository

[README.md](../README.md):

- Before: “This repository contains a client library and a few handy utilities for easy access to all of the capabilities of B2 Cloud Storage.”
- After: “This repository contains a client library and a few handy utilities. They provide easy access to all capabilities of B2 Cloud Storage.”

[doc/source/tutorial.rst](../doc/source/tutorial.rst):

- Before: “With the ``info`` object in hand, we can now proceed to create a ``B2Api`` object.”
- After: “Use the ``info`` object to create a ``B2Api`` object.”

[CONTRIBUTING.md](../CONTRIBUTING.md):

- Before: “Integration tests need real B2 credentials; see the published guide for details.”
- After: “Integration tests need real B2 credentials. See the published guide for details.”

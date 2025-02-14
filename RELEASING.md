## Overview

This document explains the release strategy for `openesarch-spark` repository.

## Branching

This repository takes similar branching strategy as other OpenSearch projects ([ref](https://github.com/opensearch-project/.github/blob/main/RELEASING.md#branching)), but does not track OpenSearch version (e.g. 1.x, 2.x, 3.x, etc.)

* **main**: The _next major_ release, currently 1.0. This is the branch where all merges take place, and code moves fast.
* * **0.x**: The _next minor_ release. Once a change is merged into `main`, decide whether to backport it to `0.x`.
* * **0.6**: The _last minor_ release and the _next patch_ release. Once a change is merged into `0.x`, it may be backported to.

### Feature Branches

Do not create branches in the upstream repo, use your fork, for the exception of long lasting feature branches that require active collaboration from multiple developers. Name feature branches `feature/<thing>`. Once the work is merged to `main`, please make sure to delete the feature branch.

### Backporting

Backwards-incompatible changes always result in a new major version and will __never__ be backported. Small improvements and features will be backported to a new minor version (e.g. `0.6`). High severity security or critical bug fixes will be backported to a new patch version (e.g. `0.6.1`). This repo use a backport workflow where you can label a PR, e.g. `backport 0.x`, and the workflow will attempt an automatic backport and open a new PR. If the backport fails, it is the contributor's responsibility to do a manual backport following the instructions in the failed backport error message.

## Versioning

A user-facing breaking change can only be made in a major release. Any regression that breaks SemVer is considered a high severity bug.


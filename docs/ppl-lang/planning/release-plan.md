# PPL Release Plan 
The next doc present a release plan summarizing the commands / features planned for the upcoming releases.

Each release will have a list of all the next parts:
 - commands / functions
 - limitations:
   - deprecated
   - breaking-changes
 - comments

### Command metadata categories

- `experimental`: Commands that are new, not fully tested, or still in the final stages of development. These features may change or be removed in future releases without warning.

- `stable`: Commands that are fully tested, documented, and considered reliable for general use. These features are expected to remain consistent across releases, with no breaking changes unless explicitly stated.

- `deprecated`: Commands that are planned for removal in future releases. While they may still work, their usage is discouraged, and alternative stable commands should be used when available.


Each command / function would have the following properties:
 - `status`: the maturity of the feature (experimental, stable, deprecated)
 - `since`: the version it was introduced 

#### SemVer Support in PPL Releases:
Each release will follow the SemVer versioning system:

- `MAJOR` version when there are incompatible API changes or breaking changes to existing commands and functionality.
- `MINOR` version when adding new features in a backward-compatible manner, including new commands or enhancements to the existing system.
- `PATCH` version when making backward-compatible bug fixes, performance improvements, or minor updates.

---
## [Version 0.4.1](https://github.com/opensearch-project/opensearch-spark/releases/tag/v0.4.1)

**Status**: stable

### Commands
| command           | status       | since | doc |
|-------------------|--------------|-------|-----|
| fields            | stable       | 0.4   |[ppl-fields-command.md](../commands/ppl-fields-command.md)|
| search            | stable       | 0.4   |[ppl-search-command.md](../commands/ppl-search-command.md)|
| sort              | stable       | 0.4   |[ppl-sort-command.md](../commands/ppl-sort-command.md)|
| stats             | stable       | 0.4   |[ppl-stats-command.md](../commands/ppl-stats-command.md)|
| stats + span      | stable       | 0.4   |[ppl-stats-command.md](../commands/ppl-stats-command.md)|
| stats + span + by | stable       | 0.4   |[ppl-stats-command.md](../commands/ppl-stats-command.md)|
| where             | stable       | 0.4   |[ppl-where-command.md](../commands/ppl-where-command.md)|
| head              | stable       | 0.4   |[ppl-head-command.md](../commands/ppl-head-command.md)|
| correlation       | experimental | 0.4   |[ppl-correlation-command.md](../commands/ppl-correlation-command.md)|

### Functions

| function           | status  | since | doc |
|--------------------|---------|-------|-----|
| logical conditions | stable  | 0.4   |[ppl-expressions.md](../functions/ppl-expressions.md)|


---
## [Version 0.5.0](https://github.com/opensearch-project/opensearch-spark/releases/tag/v0.5.0)

**Status**: latest

### Commands
| command           | status       | since | doc |
|-------------------|--------------|-------|-----|
| fields            | stable       | 0.4   |[ppl-fields-command.md](../commands/ppl-fields-command.md)|
| search            | stable       | 0.4   |[ppl-search-command.md](../commands/ppl-search-command.md)|
| sort              | stable       | 0.4   |[ppl-sort-command.md](../commands/ppl-sort-command.md)|
| stats             | stable       | 0.4   |[ppl-stats-command.md](../commands/ppl-stats-command.md)|
| stats + span      | stable       | 0.4   |[ppl-stats-command.md](../commands/ppl-stats-command.md)|
| stats + span + by | stable       | 0.4   |[ppl-stats-command.md](../commands/ppl-stats-command.md)|
| where             | stable       | 0.4   |[ppl-where-command.md](../commands/ppl-where-command.md)|
| head              | stable       | 0.4   |[ppl-head-command.md](../commands/ppl-head-command.md)|
| correlation       | experimental | 0.4   |[ppl-correlation-command.md](../commands/ppl-correlation-command.md)|
| explain           | experimental | 0.5   ||
| eval              | experimental | 0.5   |[ppl-eval-command.md](../commands/ppl-eval-command.md)|
| dedup             | experimental | 0.5   |[ppl-dedup-command.md](../commands/ppl-dedup-command.md)|
| describe          | experimental | 0.5   ||
| nested-fields     | experimental | 0.5   |[ppl-subquery-command.md](../commands/ppl-subquery-command.md)|
| grok              | experimental | 0.5   |[ppl-grok-command.md](../commands/ppl-grok-command.md)|
| parse             | experimental | 0.5   |[ppl-parse-command.md](../commands/ppl-parse-command.md)|
| patterns          | experimental | 0.5   |[ppl-patterns-command.md](../commands/ppl-patterns-command.md)|
| rename            | experimental | 0.5   |[ppl-rename-command.md](../commands/ppl-rename-command.md)|
| rare              | experimental | 0.5   |[ppl-rare-command.md](../commands/ppl-rare-command.md)|
| top               | experimental | 0.5   |[ppl-top-command.md](../commands/ppl-top-command.md)|


### Functions

| function           | status       | since | doc |
|--------------------|--------------|-------|-----|
| logical conditions | stable       | 0.4   |[ppl-expressions.md](../functions/ppl-expressions.md)|
| case               | experimental | 0.5   ||
| if                 | experimental | 0.5   || 
| filed (- / +)      | experimental | 0.5   ||

### Limitations
- `fields - list` shows incorrect results for spark version 3.5.1 - see [issue](https://github.com/opensearch-project/opensearch-spark/pull/732)
- `eval` with comma separated expression needs spark version >= 3.5.1
- `rename` needs spark version >= 3.5.1
- `dedup` command with `allowedDuplication > 1` feature needs spark version >= 3.5.1

---
## Version 0.6.0 (pre-release)
**Status**: planned

### Commands
| command               | status       | since | doc                                                                  |
|-----------------------|--------------|-------|----------------------------------------------------------------------|
| fields            | stable       | 0.4   | [ppl-fields-command.md](../commands/ppl-fields-command.md)           |
| search            | stable       | 0.4   | [ppl-search-command.md](../commands/ppl-search-command.md)           |
| sort              | stable       | 0.4   | [ppl-sort-command.md](../commands/ppl-sort-command.md)               |
| stats             | stable       | 0.4   | [ppl-stats-command.md](../commands/ppl-stats-command.md)             |
| stats + span      | stable       | 0.4   | [ppl-stats-command.md](../commands/ppl-stats-command.md)             |
| stats + span + by | stable       | 0.4   | [ppl-stats-command.md](../commands/ppl-stats-command.md)             |
| where             | stable       | 0.4   | [ppl-where-command.md](../commands/ppl-where-command.md)             |
| head              | stable       | 0.4   | [ppl-head-command.md](../commands/ppl-head-command.md)               |
| correlation       | experimental | 0.4   | [ppl-correlation-command.md](../commands/ppl-correlation-command.md) |
| explain           | experimental | 0.5   |                                                                      |
| eval              | experimental | 0.5   | [ppl-eval-command.md](../commands/ppl-eval-command.md)               |
| dedup             | experimental | 0.5   | [ppl-dedup-command.md](../commands/ppl-dedup-command.md)             |
| describe          | experimental | 0.5   |                                                                      |
| nested-fields     | experimental | 0.5   | [ppl-subquery-command.md](../commands/ppl-subquery-command.md)       |
| grok              | experimental | 0.5   | [ppl-grok-command.md](../commands/ppl-grok-command.md)               |
| parse             | experimental | 0.5   | [ppl-parse-command.md](../commands/ppl-parse-command.md)             |
| patterns          | experimental | 0.5   | [ppl-patterns-command.md](../commands/ppl-patterns-command.md)       |
| rename            | experimental | 0.5   | [ppl-rename-command.md](../commands/ppl-rename-command.md)           |
| rare              | experimental | 0.5   | [ppl-rare-command.md](../commands/ppl-rare-command.md)               |
| top               | experimental | 0.5   | [ppl-top-command.md](../commands/ppl-top-command.md)                 |
| join                  | experimental | 0.6   | [ppl-join-command.md](../commands/ppl-join-command.md)               |
| lookup                | experimental | 0.6   | [ppl-lookup-command.md](../commands/ppl-lookup-command.md)           |
| inSubQuery (subQuery) | experimental | 0.6   | [ppl-subquery-command.md](../commands/ppl-subquery-command.md)       |
| help                  | experimental | 0.6   |                                                                      |
| fieldsummary          | experimental | 0.6   | WIP                                                                  |
| trendline             | experimental | 0.6   | WIP                                                                  |
| fillnull              | experimental | 0.6   | WIP                                                                  |
| flatten               | experimental | 0.6   | WIP                                                                  |
| expand_field          | experimental | 0.6   | WIP                                                                  |
| CIDIR (IP)            | experimental | 0.6   | WIP                                                                  |
| IP Location           | experimental | 0.6   | WIP                                                                  |
### Functions

| function           | status       | since | doc |
|--------------------|--------------|-------|----|
| logical conditions | stable       | 0.4   |[ppl-expressions.md](../functions/ppl-expressions.md)|
| case               | experimental | 0.5   ||
| if                 | experimental | 0.5   ||
| isEmpty            | experimental | 0.5   ||
| isPresent          | experimental | 0.5   ||
| filed (- / +)      | experimental | 0.5   ||
| isBlank            | experimental | 0.6   ||
| coalesce           | experimental | 0.6   ||
| between            | experimental | 0.7   ||

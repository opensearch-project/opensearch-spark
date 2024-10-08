# PPL Release Plan 
The next doc present a release plan summarizing the commands / features planned for the upcoming releases.

Each release will have a list of all the next parts:
 - commands / functions
 - limitations
 - comments

Each command / function would have the following properties:
 - `status`: the maturity of the feature (experimental, stable, deprecated)
 - `since`: the version it was introduced 

---
## Version 0.4

### Commands
| command           | status       | since |
|-------------------|--------------|-------|
| fields            | stable       | 0.4   |
| search            | stable       | 0.4   |
| sort              | stable       | 0.4   |
| stats             | stable       | 0.4   |
| stats + span      | stable       | 0.4   |
| stats + span + by | stable       | 0.4   |
| where             | stable       | 0.4   |
| head              | stable       | 0.4   |
| correlation       | experimental | 0.4   |

### Functions

| function           | status  | since |
|--------------------|---------|-------|
| logical conditions | stable  | 0.4   |


---
## Version 0.5

### Commands
| command           | status       | since |
|-------------------|--------------|-------|
| fields            | stable       | 0.4   |
| search            | stable       | 0.4   |
| sort              | stable       | 0.4   |
| stats             | stable       | 0.4   |
| stats + span      | stable       | 0.4   |
| stats + span + by | stable       | 0.4   |
| where             | stable       | 0.4   |
| head              | stable       | 0.4   |
| correlation       | experimental | 0.4   |
| explain           | experimental | 0.5   |
| eval              | experimental | 0.5   |
| dedup             | stable       | 0.5   |
| describe          | stable       | 0.5   |
| nested-fields     | experimental | 0.5   |
| grok              | experimental | 0.5   |
| parse             | experimental | 0.5   |
| patterns          | experimental | 0.5   |
| rename            | experimental | 0.5   |
| rare              | experimental | 0.5   |
| top               | experimental | 0.5   |


### Functions

| function           | status       | since |
|--------------------|--------------|-------|
| logical conditions | stable       | 0.4   |
| case               | experimental | 0.5   |
| if                 | experimental | 0.5   | 
| filed (- / +)      | experimental | 0.5   | 

### Limitations
- `fields - list` shows incorrect results for spark version 3.3 - see [issue](https://github.com/opensearch-project/opensearch-spark/pull/732)
- `eval` with comma separated expression needs spark version >= 3.4
- `dedup` command with `allowedDuplication > 1` feature needs spark version >= 3.4

---
## Version 0.6

### Commands
| command               | status       | since |
|-----------------------|--------------|-------|
| fields                | stable       | 0.4   |
| search                | stable       | 0.4   |
| sort                  | stable       | 0.4   |
| stats                 | stable       | 0.4   |
| stats + span          | stable       | 0.4   |
| stats + span + by     | stable       | 0.4   |
| where                 | stable       | 0.4   |
| head                  | stable       | 0.4   |
| correlation           | experimental | 0.4   |
| explain               | experimental | 0.5   |
| eval                  | experimental | 0.5   |
| dedup                 | stable       | 0.5   |
| describe              | stable       | 0.5   |
| nested-fields         | experimental | 0.5   |
| grok                  | experimental | 0.5   |
| parse                 | experimental | 0.5   |
| patterns              | experimental | 0.5   |
| rename                | experimental | 0.5   |
| rare                  | experimental | 0.5   |
| top                   | experimental | 0.5   |
| join                  | experimental | 0.6   |
| lookup                | experimental | 0.6   |
| inSubQuery (subQuery) | experimental | 0.6   |
| help                  | experimental | 0.6   |
| fieldsummary          | experimental | 0.6   |


### Functions

| function           | status       | since |
|--------------------|--------------|-------|
| logical conditions | stable       | 0.4   |
| case               | experimental | 0.5   |
| if                 | experimental | 0.5   |
| isEmpty            | experimental | 0.5   |
| isPresent          | experimental | 0.5   |
| filed (- / +)      | experimental | 0.5   | 
| isBlank            | experimental | 0.6   |
| coalesce           | experimental | 0.6   |

---
## Version 0.7

### Commands
| command               | status       | since |
|-----------------------|--------------|-------|
| fields                | stable       | 0.4   |
| search                | stable       | 0.4   |
| sort                  | stable       | 0.4   |
| stats                 | stable       | 0.4   |
| stats + span          | stable       | 0.4   |
| stats + span + by     | stable       | 0.4   |
| where                 | stable       | 0.4   |
| head                  | stable       | 0.4   |
| correlation           | experimental | 0.4   |
| explain               | experimental | 0.5   |
| eval                  | experimental | 0.5   |
| dedup                 | stable       | 0.5   |
| describe              | stable       | 0.5   |
| nested-fields         | experimental | 0.5   |
| grok                  | experimental | 0.5   |
| parse                 | experimental | 0.5   |
| patterns              | experimental | 0.5   |
| rename                | experimental | 0.5   |
| rare                  | experimental | 0.5   |
| top                   | experimental | 0.5   |
| join                  | experimental | 0.6   |
| lookup                | experimental | 0.6   |
| inSubQuery (subQuery) | experimental | 0.6   |
| help                  | experimental | 0.6   |
| fieldsummary          | experimental | 0.6   |
| input                 | experimental | 0.7   |
| file  (as source)     | experimental | 0.7   |
| trendline             | experimental | 0.7   |
| fillnull              | experimental | 0.7   |
| flatten               | experimental | 0.7   |
| expand_field          | experimental | 0.7   |
| expand_field          | experimental | 0.7   |
| CIDIR (IP)            | experimental | 0.7   |
| IP Location           | experimental | 0.7   |

### Functions

| function           | status       | since |
|--------------------|--------------|-------|
| logical conditions | stable       | 0.4   |
| case               | experimental | 0.5   |
| if                 | experimental | 0.5   |
| isEmpty            | experimental | 0.5   |
| isPresent          | experimental | 0.5   |
| filed (- / +)      | experimental | 0.5   | 
| isBlank            | experimental | 0.6   |
| coalesce           | experimental | 0.6   |
| between            | experimental | 0.7   |


---
## Version 0.7

### Commands

### Functions

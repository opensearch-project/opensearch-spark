# OpenSearch PPL Command Development Process
This document outlines the formal process for proposing and implementing new PPL commands or syntax changes in OpenSearch.
## Phase 1: Proposal
### 1.1 Create GitHub Issue

Start by creating a new GitHub issue using the following [template](.github/ISSUE_TEMPLATE/ppl_command_request.md):
```
name: PPL Command request
about: Request a new PPL command Or syntax change
title: '[PPL-Lang]'
labels: 'enhancement, untriaged'
assignees: ''
---

**Is your feature request related to a problem?**
A clear and concise description of what the PPL command/syntax change is about, why is it needed, e.g. _I'm always frustrated when [...]_

**What solution would you like?**
A clear and concise description of what you want to happen.
- Add Example new / updated syntax
- [Optional] Add suggested [ANTLR](https://www.antlr.org/) suggested grammar

**Add Proposal Document**
Under the [docs/planning](../../docs/ppl-lang/planning) folder add a dedicated page for your suggested command or syntax change

See [ppl-fillnull-command.md](../../docs/ppl-lang/planning/ppl-fillnull-command.md) example

**Do you have any additional context?**
Add any other context or screenshots about the feature request here.
```

### 1.2 Create Planning Document PR
Create a Pull Request that adds a new markdown file under the `docs/ppl-lang/planning folder`. This document should include:

1) Command overview and motivation
2) Detailed syntax specification 
3) Example usage scenarios 
4) Implementation considerations 
5) Potential limitations or edge cases

### Phase 2: Review and Approval

1) Community members and maintainers review the proposal 
2) Feedback is incorporated into the planning document / PR comments
3) Proposal is either accepted, rejected, or sent back for revision

### Phase 3: Experimental Implementation
Once approved, the command enters the experimental phase:

1) Create implementation PR with:
    - Code changes
    - Comprehensive test suite
    - Documentation updates

2) Code is clearly marked as experimental using appropriate annotations 
3) Documentation indicates experimental status 
4) Experimental features are disabled by default in production

### Phase 4: Maturation
During the experimental phase:

1) Gather user feedback 
2) Address issues and edge cases 
3) Refine implementation and documentation 
4) Regular review of usage and stability

### Phase 5: Formal Integration
When the command has matured:

1) Create PR to remove experimental status
2) Update all documentation to reflect stable status
3) Ensure backward compatibility
4) Merge into main PPL command set

### Best Practices

* Follow existing PPL command patterns and conventions
* Ensure comprehensive test coverage
* Provide clear, detailed documentation with examples
* Consider performance implications
* Maintain backward compatibility when possible

### Timeline Expectations

* Proposal Review: 1-2 weeks
* Experimental Phase: 1-3 months
* Maturation to Formal Integration: Based on community feedback and stabilityOpenSearch PPL Command Development Process

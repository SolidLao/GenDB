# Task: Review C++ query code for {{query_id}}

## Code File
Read: {{cpp_path}}

## Experience Skill
Read: {{experience_path}}

{{#if query_guide}}
## Query Guide (Column Reference)
{{query_guide}}

Verify that the code's constants (filter thresholds, scale divisors, dict patterns)
match the Column Reference above. Flag mismatches as critical issues.
{{/if}}

{{#if previous_passing_cpp}}
## Previous Passing Code (for C13-C15 regression detection)
Compare against: {{previous_passing_cpp}}
Flag any changes to date constants, scale thresholds, or revenue formulas.
{{/if}}

Check the code against ALL entries in the experience skill.
Output your review as a JSON block with verdict and issues.

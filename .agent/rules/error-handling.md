---
trigger: always_on
---

Error Handling Standards

No Bare Excepts: Never use except: without an exception type. Catch specific errors (e.g., except ValueError:).
Structured Logging: Do not use print(). Use the logging library for all outputs.
Fail Gracefully: Scripts should never crash with a stack trace visible to the user. Wrap main execution in a try/except block.
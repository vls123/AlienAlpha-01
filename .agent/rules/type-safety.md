---
trigger: always_on
---

Type Safety Rules

Strict Typing: All function signatures must have type annotations.
No Any: Avoid using Any type. Define data classes or interfaces for complex structures.
Return Types: Always specify the return type, even if it is None or void.
Test It: Ask the agent: "Create a function that processes a list of user dictionaries."
Result: Instead of generic Dicts, the agent likely defines a User TypedDict or dataclass and uses List[User] in the signature.
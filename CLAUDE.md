# CLAUDE.md

## Communication Style
- Be concise and direct
- Skip unnecessary explanations
- Get straight to the point
- No fluff or filler text

## Code Guidelines
- Write minimal, clean code
- Only include what's necessary to solve the problem
- Prefer simple solutions over complex ones
- Remove redundant code
- Use clear, short variable names
- Avoid over-engineering
- Always add docstrings and type hints for functions

## Response Format
- Lead with the solution
- Minimal context unless requested
- Code first, explanation only if needed
- No verbose introductions or conclusions

## Examples

**Good:**
```python
def sum_list(nums):
    """
    Returns the sum of numbers in a list.
    """
    return sum(nums)
```

**Avoid:**
```python
def calculate_sum_of_numbers_in_list(list_of_numbers):
    """
    This function takes a list of numbers as input and returns the sum
    of all numbers in the list using Python's built-in sum function
    """
    total_sum = 0
    for individual_number in list_of_numbers:
        total_sum = total_sum + individual_number
    return total_sum
```

## Key Principles
- Less is more
- Clarity over cleverness
- Function over form
- Direct communication
- Minimal viable solution
## Contributing to JonoonDB
When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project's open source license and warrant that you have the legal authority to do so.

## Ways to contribute
There are number of ways in which you can contribute:

1. Submit bugs/issues when you experience them.
2. Help other users of JonoonDB when they post questions on stack overflow or on our google group.
3. Build complementary libraries or products for JonoonDB.
4. Submit pull requests with bug fixes.
5. Contribute code for new features. But before you embark on this please post your ideas on our google group. Sometimes the feature you want to work on may already be in progress. At other times it may be implemented in a better way if discussed before implementation.

## C++ style guide 
We use [google's c++ style guide](https://google.github.io/styleguide/cppguide.html) for the most part.
Code formatting is enforced through the clang-format tool that uses the .clang-format file located at the repository root. You would need to install clang-format and git-clang-format tools for formatting. There is a pre-commit hook for formatting as well that you can activate by running the following command:

`git config core.hooksPath .githooks`

This hook will invoke git-clang-format, to ensure that all your changes are formatted, before each commit.

The places where we differ from google style guide are documented below:

1. Instance/Member variables should being with "m_". It works with intellisense better as you are trying to use the member variables.
2. Variable names such as parameters, local function variables, instance variables should use camelCase instead of snake_case.
3. Struct variable are named exactly like class variables except that they don't have "m_" prefix.
4. Prefer using C++ headers for C functions and types e.g. prefer <cstring> over <string.h>, <cstdint> over <stdint.h> etc.
5. Use #pragma once instead of header guards because all major compilers support it.
6. We use exceptions for error handling instead of error codes.

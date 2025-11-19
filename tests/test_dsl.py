import pytest
from pyrqg.dsl.core import Grammar, Template


def test_template_raises_on_unresolved_placeholder():
    """
    Verify that Template.generate raises a ValueError if a placeholder
    cannot be resolved.
    """
    g = Grammar()
    g.rule("greeting", Template("Hello, {name} and {unresolved_friend}"))
    g.rule("name", "World")

    with pytest.raises(ValueError) as excinfo:
        g.generate("greeting")

    # Check that the error message is informative
    assert "Unresolved placeholders" in str(excinfo.value)
    assert "unresolved_friend" in str(excinfo.value)


def test_template_resolves_correctly():
    """
    Verify that a valid template still works correctly.
    """
    g = Grammar()
    g.rule("greeting", Template("Hello, {name}"))
    g.rule("name", "World")

    result = g.generate("greeting")
    assert result == "Hello, World"

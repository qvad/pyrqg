import pytest

from pyrqg.runner import build_parser


def test_cli_parses_list_subcommand():
    """
    Tests that the argparse parser correctly handles the 'list' subcommand.
    """
    parser = build_parser()
    args = parser.parse_args(['list'])
    assert args.mode == 'list'


def test_cli_parses_grammar_subcommand():
    """
    Tests that the 'grammar' subcommand parses its specific arguments.
    """
    parser = build_parser()
    args = parser.parse_args([
        'grammar',
        '--grammar', 'my_grammar',
        '--count', '50',
        '--seed', '123',
    ])
    assert args.mode == 'grammar'
    assert args.grammar == 'my_grammar'
    assert args.count == 50
    assert args.seed == 123


def test_cli_grammar_requires_argument():
    """
    Tests that the 'grammar' subcommand fails if --grammar is not provided.
    """
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['grammar'])

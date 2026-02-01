import pytest
from pyrqg.runner import build_parser

def test_cli_parses_list_subcommand():
    parser = build_parser()
    args = parser.parse_args(['list'])
    assert args.mode == 'list'


def test_cli_parses_runners_subcommand():
    parser = build_parser()
    args = parser.parse_args(['runners'])
    assert args.mode == 'runners'


def test_cli_parses_grammar_subcommand():
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


def test_cli_grammar_requires_argument(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['grammar'])
    capsys.readouterr()

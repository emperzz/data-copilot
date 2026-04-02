from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware


class TestClarificationMiddlewareFormatOptions:
    def test_formats_options_list(self):
        mw = ClarificationMiddleware()
        formatted = mw._format_clarification_message(  # noqa: SLF001
            {
                "question": "Q?",
                "clarification_type": "approach_choice",
                "options": ["opt-a", "opt-b"],
            }
        )
        assert "  1. opt-a" in formatted
        assert "  2. opt-b" in formatted

    def test_formats_options_json_string(self):
        mw = ClarificationMiddleware()
        formatted = mw._format_clarification_message(  # noqa: SLF001
            {
                "question": "Q?",
                "clarification_type": "approach_choice",
                "options": '["检查SQL语法","转换为其他"]',
            }
        )
        assert "  1. 检查SQL语法" in formatted
        assert "  2. 转换为其他" in formatted


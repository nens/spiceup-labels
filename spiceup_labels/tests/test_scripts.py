# -*- coding: utf-8 -*-
"""Tests for script.py"""
from spiceup_labels import patch_calendar_tasks


def test_get_parser():
    parser = patch_calendar_tasks.get_parser()
    # As a test, we just check one option. That's enough.
    options = parser.parse_args()
    assert options.verbose == False

def test_main():
    r_status_code = patch_calendar_tasks.main()
    assert r_status_code == 200
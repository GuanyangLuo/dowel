import csv
import tempfile

import pytest

from dowel import CsvOutput, TabularInput
from dowel.csv_output import CsvOutputWarning


class TestCsvOutput:

    def setup_method(self):
        self.log_file = tempfile.NamedTemporaryFile()
        self.csv_output = CsvOutput(self.log_file.name)
        self.tabular = TabularInput()
        self.tabular.clear()

    def teardown_method(self):
        self.log_file.close()

    def test_record(self):
        foo = 1
        bar = 10
        self.tabular.record('foo', foo)
        self.tabular.record('bar', bar)
        self.csv_output.record(self.tabular)
        self.tabular.record('foo', foo * 2)
        self.tabular.record('bar', bar * 2)
        self.csv_output.record(self.tabular)
        self.csv_output.dump()

        correct = [
            {'foo': str(foo), 'bar': str(bar)},
            {'foo': str(foo * 2), 'bar': str(bar * 2)},
        ]  # yapf: disable
        self.assert_csv_matches(correct)

    def test_record_inconsistent(self):
        foo = 1
        bar = 10
        self.tabular.record('foo', foo)
        self.csv_output.record(self.tabular)
        self.tabular.record('foo', foo * 2)
        self.tabular.record('bar', bar * 2)

        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        # this should not produce a warning, because we only warn once
        self.csv_output.record(self.tabular)

        self.csv_output.dump()

        correct = [
            {'foo': str(foo)},
            {'foo': str(foo * 2)},
            {'foo': str(foo * 2)},
        ]  # yapf: disable
        self.assert_csv_matches(correct)

    def test_record_stale(self):
        foo = 1
        bar = 10
        self.tabular.record('foo', foo)
        self.tabular.record('bar', bar)
        self.csv_output.record(self.tabular)
        self.tabular.record('foo', foo * 2)
        self.csv_output.record(self.tabular)

        self.csv_output.dump()

        # expected = [
        #     {'foo': str(foo), 'bar': str(bar)},
        #     {'foo': str(foo * 2), 'bar': ''},
        # ]  # yapf: disable

        stale = [
            {'foo': str(foo), 'bar': str(bar)},
            {'foo': str(foo * 2), 'bar': str(bar)},
        ]  # yapf: disable
        self.assert_csv_matches(stale)

    def test_record_nonprimitive(self):
        foo = 1
        bar = 10
        self.tabular.record('foo', foo)
        self.tabular.record('bar', bar)
        self.csv_output.record(self.tabular)
        self.tabular.record('foo', foo * 2)
        self.tabular.record('bar', None)

        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.csv_output.dump()

        correct = [
            {'foo': str(foo), 'bar': str(bar)},
            {'foo': str(foo * 2), 'bar': ''},
        ]  # yapf: disable
        self.assert_csv_matches(correct)

    def test_record_inconsistency_handling_copy(self):
        self.csv_output._inconsistency_handling = 'copy'

        a = 1
        b = 10
        c = 100
        d = 1000
        x = -1

        self.tabular.record('a', a)
        self.tabular.record('b', b)
        self.tabular.record('c', c)
        self.csv_output.record(self.tabular)

        self.tabular.record('a', a * 2)
        self.tabular.record('b', None)
        self.tabular.record('c', c * 2)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.tabular.record('a', None)
        self.tabular.record('b', None)
        self.tabular.record('c', None)
        self.tabular.record('x', x * 3)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.tabular.record('b', b * 4)
        self.tabular.record('x', x * 4)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.tabular.record('b', b * 5)
        self.tabular.record('x', x * 5)
        self.tabular.record('d', d * 5)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.csv_output.dump()

        self.csv_output.close()

        correct = [
            {'a': str(a), 'b': str(b), 'c': str(c), 'x': '', 'd': ''},
            {'a': str(a * 2), 'b': '', 'c': str(c * 2), 'x': '', 'd': ''},
            {'a': '', 'b': '', 'c': '', 'x': str(x * 3), 'd': ''},
            {'a': '', 'b': str(b * 4), 'c': '', 'x': str(x * 4), 'd': ''},
            {'a': '', 'b': str(b*5), 'c': '', 'x': str(x*5), 'd': str(d*5)},
        ]  # yapf: disable
        self.assert_csv_matches(correct)

    def test_record_inconsistency_handling_fixed_header_length(self):
        self.csv_output._inconsistency_handling = 'fixed_header_length'

        a = 1
        b = 10
        c = 100
        d = 1000
        x = -1

        self.tabular.record('a', a)
        self.tabular.record('b', b)
        self.tabular.record('c', c)
        self.csv_output.record(self.tabular)

        self.tabular.record('a', a * 2)
        self.tabular.record('b', None)
        self.tabular.record('c', c * 2)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.tabular.record('a', None)
        self.tabular.record('b', None)
        self.tabular.record('c', None)
        self.tabular.record('x', x * 3)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.tabular.record('b', b * 4)
        self.tabular.record('x', x * 4)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.tabular.record('b', b * 5)
        self.tabular.record('x', x * 5)
        self.tabular.record('d', d * 5)
        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.csv_output.dump()

        correct = [
            {'a': str(a), 'b': str(b), 'c': str(c), 'x': None, 'd': None},
            {'a': str(a * 2), 'b': '', 'c': str(c * 2), 'x': None, 'd': None},
            {'a': '', 'b': '', 'c': '', 'x': str(x * 3), 'd': None},
            {'a': '', 'b': str(b * 4), 'c': '', 'x': str(x * 4), 'd': None},
            {'a': '', 'b': str(b*5), 'c': '', 'x': str(x*5), 'd': str(d*5)},
        ]  # yapf: disable
        self.assert_csv_matches(correct)

    def test_record_fixed_header_length_long(self):
        self.csv_output._inconsistency_handling = 'fixed_header_length'

        foo = 1
        bar = 10
        bar_long = 'bar' * self.csv_output._header_length
        # 'foo,' takes 4 characters
        bar_long_truncated = bar_long[:self.csv_output._header_length - 4]
        self.tabular.record('foo', foo)
        self.tabular.record(bar_long, bar)
        self.csv_output.record(self.tabular)
        self.tabular.record('foo', foo * 2)
        self.tabular.record(bar_long, None)

        with pytest.warns(CsvOutputWarning):
            self.csv_output.record(self.tabular)

        self.csv_output.dump()

        correct = [
            {'foo': str(foo), bar_long_truncated: str(bar)},
            {'foo': str(foo * 2), bar_long_truncated: ''},
        ]  # yapf: disable
        self.assert_csv_matches(correct)

    def test_empty_record(self):
        self.csv_output.record(self.tabular)
        assert not self.csv_output._writer

        foo = 1
        bar = 10
        self.tabular.record('foo', foo)
        self.tabular.record('bar', bar)
        self.csv_output.record(self.tabular)
        assert not self.csv_output._warned_once

    def test_unacceptable_type(self):
        with pytest.raises(ValueError):
            self.csv_output.record('foo')

    def test_disable_warnings(self):
        foo = 1
        bar = 10
        self.tabular.record('foo', foo)
        self.csv_output.record(self.tabular)
        self.tabular.record('foo', foo * 2)
        self.tabular.record('bar', bar * 2)

        self.csv_output.disable_warnings()

        # this should not produce a warning, because we disabled warnings
        self.csv_output.record(self.tabular)

    def assert_csv_matches(self, correct):
        """Check the first row of a csv file and compare it to known values."""
        with open(self.log_file.name, 'r') as file:
            reader = csv.DictReader(file)
            if self.csv_output._inconsistency_handling == \
                    'fixed_header_length':
                reader.fieldnames[-1] = reader.fieldnames[-1].rstrip()

            for correct_row in correct:
                row = next(reader)
                assert row == correct_row

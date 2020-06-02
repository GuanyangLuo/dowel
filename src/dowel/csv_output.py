"""A `dowel.logger.LogOutput` for CSV files."""
import csv
import fileinput
import warnings

from dowel import TabularInput
from dowel.simple_outputs import FileOutput
from dowel.utils import colorize


class CsvOutput(FileOutput):
    """CSV file output for logger.

    :param file_name: The file this output should log to.
    :param implementation (str): Specify which implementation to use for
        handling inconsistent TabularInput keys. (Default it None.)

        For the 'copy' implementation, the complete header is written when
        closing the output. Data rows are copied and rewritten as if they were
        written with the complete header / fieldnames (i.e. missing keys will
        have '' as the values).

        For the 'fixed_header_length' implementation, only the header is
        rewritten when there is a new key. This may be more efficient than
        'copy' for large amount of data, but it requires allocating enough
        characters for the header line, due to the nature of file IO. The
        trailing whitespace for the last fieldname needs to be stripped when
        using csv.DictReader().
    :param header_length (int): The header length (number of characters) for
        the 'fixed_header_length' implementation. (Default is 140.)
        Header shorter than the header_length will be padded with whitespace.
        Header longer than the header_length will be truncated.
    """

    def __init__(self, file_name, implementation=None, header_length=140):
        self._inconsistency_handling = implementation
        self._header_length = header_length
        super().__init__(file_name)
        self._writer = None
        self._fieldnames = None
        self._warned_once = set()
        self._disable_warnings = False

    @property
    def types_accepted(self):
        """Accept TabularInput objects only."""
        return (TabularInput, )

    def record(self, data, prefix=''):
        """Log tabular data to CSV."""
        if isinstance(data, TabularInput):
            to_csv = data.as_primitive_dict

            if not to_csv.keys() and not self._writer:
                return

            if not self._writer:
                self._fieldnames = set(to_csv.keys())
                fieldnames_ordered = list(to_csv.keys())
                self._writer = csv.DictWriter(self._log_file,
                                              fieldnames=fieldnames_ordered,
                                              extrasaction='ignore')
                if self._inconsistency_handling == 'fixed_header_length':
                    # Manually write the header with padding
                    header = ','.join(fieldnames_ordered)
                    header_padded = '{:<{}}'.format(header,
                                                    self._header_length)
                    header_trucated = header_padded[:self._header_length] \
                        + '\r\n'  # Default line ending for csv writer
                    self._log_file.write(header_trucated)
                else:
                    self._writer.writeheader()

            if to_csv.keys() != self._fieldnames:
                # May need to change the content of the warning
                # to reflect new handling of inconsistent TabularInput
                self._warn('Inconsistent TabularInput keys detected. '
                           'CsvOutput keys: {}. '
                           'TabularInput keys: {}. '
                           'Did you change key sets after your first '
                           'logger.log(TabularInput)?'.format(
                               set(self._fieldnames), set(to_csv.keys())))
                if self._inconsistency_handling:
                    # Check if data contains new fieldnames
                    fieldnames_new = to_csv.keys() - self._fieldnames
                    if fieldnames_new:
                        # Update the fieldnames
                        self._fieldnames.update(fieldnames_new)
                        self._writer.fieldnames.extend(list(fieldnames_new))
                        # Rewrite the header
                        if self._inconsistency_handling == \
                                'fixed_header_length':
                            self._log_file.seek(0)
                            header = ','.join(self._writer.fieldnames)
                            header_length = min(len(header),
                                                self._header_length)
                            header_trucated = header[:header_length]
                            self._log_file.write(header_trucated)
                            self._log_file.seek(0, 2)

            self._writer.writerow(to_csv)

            for k in to_csv.keys():
                data.mark(k)
        else:
            raise ValueError('Unacceptable type.')

    def close(self):
        """Close any files used by the output.

        This method will also carry out handling of inconsistent TabularInput
        keys with the 'copy' implementation, if specified for the output.
        """
        super().close()
        if self._inconsistency_handling == 'copy':
            # Rewrite the file with a complete header
            # This involves copying the content of the file
            with fileinput.input(files=self._log_file.name, inplace=True) as f:
                fieldnames = self._writer.fieldnames
                reader = csv.DictReader(f, fieldnames=fieldnames, restval='')
                header = ','.join(fieldnames)

                for row in reader:
                    if f.isfirstline():
                        print(header)
                    else:
                        # Emulate csv.DictWriter with restval=''
                        values = [row.get(key, '') for key in fieldnames]
                        row_new = ','.join(values)
                        print(row_new)

    def _warn(self, msg):
        """Warns the user using warnings.warn.

        The stacklevel parameter needs to be 3 to ensure the call to logger.log
        is the one printed.
        """
        if not self._disable_warnings and msg not in self._warned_once:
            warnings.warn(colorize(msg, 'yellow'),
                          CsvOutputWarning,
                          stacklevel=3)
        self._warned_once.add(msg)
        return msg

    def disable_warnings(self):
        """Disable logger warnings for testing."""
        self._disable_warnings = True


class CsvOutputWarning(UserWarning):
    """Warning class for CsvOutput."""

    pass

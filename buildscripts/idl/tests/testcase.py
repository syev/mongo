# Copyright (C) 2017 MongoDB Inc.
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""Utility methods and classes for testing IDL passes."""

from __future__ import absolute_import, print_function, unicode_literals

import unittest
from typing import Any, Tuple

from idl.idl import ast
from idl.idl import binder
from idl.idl import errors as _errors
from idl.idl import generator
from idl.idl import parser
from idl.idl import syntax


def errors_to_str(errors):
    # type: (_errors.ParserErrorCollection) -> unicode
    """Dump the list of errors as a multiline text string."""
    if errors is not None:
        return "\n".join(errors.to_list())
    return "<empty>"


class NothingImportResolver(parser.ImportResolverBase):
    """An import resolver that does nothing."""

    def resolve(self, base_file, imported_file_name):
        # type: (unicode, unicode) -> unicode
        """Return the complete path to an imported file name."""
        raise NotImplementedError()

    def open(self, resolved_file_name):
        # type: (unicode) -> Any
        """Return an io.Stream for the requested file."""
        raise NotImplementedError()


class IDLTestcase(unittest.TestCase):
    """IDL Test case base class."""

    def _parse(self, doc_str, resolver):
        # type: (unicode, parser.ImportResolverBase) -> syntax.IDLParsedSpec
        """Parse a document and throw a unittest failure if it fails to parse as a valid YAML document."""

        try:
            return parser.parse(doc_str, "unknown", resolver)
        except:  # pylint: disable=bare-except
            self.fail("Failed to parse document:\n%s" % (doc_str))

    def _assert_parse(self, doc_str, parsed_doc):
        # type: (unicode, syntax.IDLParsedSpec) -> None
        """Assert a document parsed correctly by the IDL compiler and returned no errors."""
        self.assertIsNone(parsed_doc.errors,
                          "Expected no parser errors\nFor document:\n%s\nReceived errors:\n\n%s" %
                          (doc_str, errors_to_str(parsed_doc.errors)))
        self.assertIsNotNone(parsed_doc.spec, "Expected a parsed doc")

    def assert_parse(self, doc_str, resolver=NothingImportResolver()):
        # type: (unicode, parser.ImportResolverBase) -> None
        """Assert a document parsed correctly by the IDL compiler and returned no errors."""
        parsed_doc = self._parse(doc_str, resolver)
        self._assert_parse(doc_str, parsed_doc)

    def assert_parse_fail(self, doc_str, error_id, multiple=False,
                          resolver=NothingImportResolver()):
        # type: (unicode, unicode, bool, parser.ImportResolverBase) -> None
        """
        Assert a document parsed correctly by the YAML parser, but not the by the IDL compiler.

        Asserts only one error is found in the document to make future IDL changes easier.
        """
        parsed_doc = self._parse(doc_str, resolver)

        self.assertIsNone(parsed_doc.spec, "Expected no parsed doc")
        self.assertIsNotNone(parsed_doc.errors, "Expected parser errors")

        # Assert that negative test cases are only testing one fault in a test.
        # This is impossible to assert for all tests though.
        self.assertTrue(
            multiple or parsed_doc.errors.count() == 1,
            "For document:\n%s\nExpected only error message '%s' but received multiple errors:\n\n%s"
            % (doc_str, error_id, errors_to_str(parsed_doc.errors)))

        self.assertTrue(
            parsed_doc.errors.contains(error_id),
            "For document:\n%s\nExpected error message '%s' but received only errors:\n %s" %
            (doc_str, error_id, errors_to_str(parsed_doc.errors)))

    def assert_bind(self, doc_str, resolver=NothingImportResolver()):
        # type: (unicode, parser.ImportResolverBase) -> ast.IDLAST
        """Assert a document parsed and bound correctly by the IDL compiler and returned no errors."""
        parsed_doc = self._parse(doc_str, resolver)
        self._assert_parse(doc_str, parsed_doc)

        bound_doc = binder.bind(parsed_doc.spec)

        self.assertIsNone(bound_doc.errors,
                          "Expected no binder errors\nFor document:\n%s\nReceived errors:\n\n%s" %
                          (doc_str, errors_to_str(bound_doc.errors)))
        self.assertIsNotNone(bound_doc.spec, "Expected a bound doc")

        return bound_doc.spec

    def assert_bind_fail(self, doc_str, error_id, resolver=NothingImportResolver()):
        # type: (unicode, unicode, parser.ImportResolverBase) -> None
        """
        Assert a document parsed correctly by the YAML parser and IDL parser, but not bound by the IDL binder.

        Asserts only one error is found in the document to make future IDL changes easier.
        """
        parsed_doc = self._parse(doc_str, resolver)
        self._assert_parse(doc_str, parsed_doc)

        bound_doc = binder.bind(parsed_doc.spec)

        self.assertIsNone(bound_doc.spec, "Expected no bound doc\nFor document:\n%s\n" % (doc_str))
        self.assertIsNotNone(bound_doc.errors, "Expected binder errors")

        # Assert that negative test cases are only testing one fault in a test.
        self.assertTrue(
            bound_doc.errors.count() == 1,
            "For document:\n%s\nExpected only error message '%s' but received multiple errors:\n\n%s"
            % (doc_str, error_id, errors_to_str(bound_doc.errors)))

        self.assertTrue(
            bound_doc.errors.contains(error_id),
            "For document:\n%s\nExpected error message '%s' but received only errors:\n %s" %
            (doc_str, error_id, errors_to_str(bound_doc.errors)))

    def assert_generate(self, doc_str, resolver=NothingImportResolver()):
        # type: (unicode, parser.ImportResolverBase) -> Tuple[unicode,unicode]
        """Assert a document parsed, bound, and generated correctly by the IDL compiler."""
        spec = self.assert_bind(doc_str, resolver)

        header = generator.generate_header_str(spec)
        source = generator.generate_source_str(spec, "fake", "fake_header")

        return (header, source)

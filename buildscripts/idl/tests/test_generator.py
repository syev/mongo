#!/usr/bin/env python2
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
"""
Test cases for IDL Generator.

This file exists to verify code coverage for the generator.py file. To run code coverage, run in the
idl base directory:

$ coverage run run_tests.py && coverage html
"""

from __future__ import absolute_import, print_function, unicode_literals

import os
import unittest


from idl.idl import compiler
from idl.tests import testcase


class TestGenerator(testcase.IDLTestcase):
    """Test the IDL Generator."""

    def test_compile(self):
        # type: () -> None
        """Exercise the code generator so code coverage can be measured."""
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        src_dir = os.path.join(
            base_dir,
            'src',
        )
        idl_dir = os.path.join(src_dir, 'mongo', 'idl')

        args = compiler.CompilerArgs()
        args.output_suffix = "_codecoverage_gen"
        args.import_directories = [src_dir]

        unittest_idl_file = os.path.join(idl_dir, 'unittest.idl')
        if not os.path.exists(unittest_idl_file):
            unittest.skip("Skipping IDL Generator testing since %s could not be found." %
                          (unittest_idl_file))
            return

        args.input_file = os.path.join(idl_dir, 'unittest_import.idl')
        self.assertTrue(compiler.compile_idl(args))

        args.input_file = unittest_idl_file
        self.assertTrue(compiler.compile_idl(args))

    def test_enum_non_const(self):
        # type: () -> None
        """Validate enums are not marked as const in getters."""
        header, _ = self.assert_generate("""
        enums:

            StringEnum:
                description: "An example string enum"
                type: string
                values:
                    s0: "zero"
                    s1: "one"
                    s2: "two"

        structs:
            one_string_enum:
                description: mock
                fields:
                    value: StringEnum
        """)

        # Look for the getter.
        # Make sure the getter is marked as const.
        # Make sure the return type is not marked as const by validating the getter marked as const
        # is the only occurrence of the word "const".
        header_lines = header.split('\n')

        found = False
        for header_line in header_lines:
            if header_line.find("getValue") > 0 \
                and header_line.find("const {") > 0 \
                and header_line.find("const {") == header_line.find("const"):
                found = True

        self.assertTrue(found, "Bad Header: " + header)


if __name__ == '__main__':

    unittest.main()

#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import textwrap
import typing as _typing

from LiuXin_alpha.customize import FileTypePlugin, numeric_version

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class HTML2ZIP(FileTypePlugin):
    """
    Follows all local links in an HTML file and creates a ZIP file containing all linked files.
    This plugin is run every time you add an HTML file to the library.
    """

    name = "HTML to ZIP"
    author = "Kovid Goyal"
    description = textwrap.dedent(
        _(
            """\
Follow all local links in an HTML file and create a ZIP \
file containing all linked files. This plugin is run \
every time you add an HTML file to the library.\
"""
        )
    )
    version = numeric_version
    file_types = {"html", "htm", "xhtml", "xhtm", "shtm", "shtml"}
    supported_platforms = ["windows", "osx", "linux"]
    on_import = True

    def run(self: _typing.Self, htmlfile: _typing.Any) -> _typing.Any:
        """Report that this plugin requires the unavailable GUI conversion engine."""
        raise RuntimeError("GUI conversion is unavailable in a headless environment.")

    def customization_help(self: _typing.Self, gui: bool = False) -> _typing.Any:
        return _(
            "Character encoding for the input HTML files. Common choices " "include: cp1252, cp1251, latin1 and utf-8."
        )

    def do_user_config(self: _typing.Self, parent: _typing.Any = None) -> _typing.Any:
        """
        This method shows a configuration dialog for this plugin. It returns
        True if the user clicks OK, False otherwise. The changes are
        automatically applied.
        :param parent:
        """
        from PyQt5.Qt import (
            QCheckBox,
            QDialog,
            QDialogButtonBox,
            QLabel,
            QLineEdit,
            Qt,
            QVBoxLayout,
        )

        config_dialog = QDialog(parent)
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        v = QVBoxLayout(config_dialog)

        def size_dialog() -> None:
            config_dialog.resize(config_dialog.sizeHint())

        button_box.accepted.connect(config_dialog.accept)
        button_box.rejected.connect(config_dialog.reject)
        config_dialog.setWindowTitle(_("Customize") + " " + self.name)
        from LiuXin_alpha.customize.ui import customize_plugin, plugin_customization

        help_text = self.customization_help(gui=True)
        help_text = QLabel(help_text, config_dialog)
        help_text.setWordWrap(True)
        help_text.setTextInteractionFlags(Qt.LinksAccessibleByMouse | Qt.LinksAccessibleByKeyboard)
        help_text.setOpenExternalLinks(True)
        v.addWidget(help_text)
        bf = QCheckBox(_("Add linked files in breadth first order"))
        bf.setToolTip(
            _(
                "Normally, when following links in HTML files"
                " calibre does it depth first, i.e. if file A links to B and "
                " C, but B links to D, the files are added in the order A, B, D, C. "
                " With this option, they will instead be added as A, B, C, D"
            )
        )
        sc = plugin_customization(self)
        if not sc:
            sc = ""
        sc = sc.strip()
        enc = sc.partition("|")[0]
        bfs = sc.partition("|")[-1]
        bf.setChecked(bfs == "bf")
        sc = QLineEdit(enc, config_dialog)
        v.addWidget(sc)
        v.addWidget(bf)
        v.addWidget(button_box)
        size_dialog()
        config_dialog.exec_()

        if config_dialog.result() == QDialog.Accepted:
            sc = six_unicode(sc.text()).strip()
            if bf.isChecked():
                sc += "|bf"
            customize_plugin(self, sc)

        return config_dialog.result()

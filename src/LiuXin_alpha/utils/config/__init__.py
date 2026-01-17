__author__ = "Big Brother Iron"

from copy import deepcopy
import optparse

from LiuXin_alpha.utils.localization import _

# optparse uses gettext.gettext rather than the builtins _, so patch it
optparse._ = _
from LiuXin_alpha.constants import __appname__, get_version


class CustomHelpFormatter(optparse.IndentedHelpFormatter):
    """
    Custom help formatter.
    """
    def format_usage(self, usage):
        from LiuXin_alpha.utils.terminal import colored

        parts = usage.split(" ")
        if parts:
            parts[0] = colored(parts[0], fg="yellow", bold=True)
        usage = " ".join(parts)
        return colored(_("Usage"), fg="blue", bold=True) + ": " + usage

    def format_heading(self, heading):
        from LiuXin_alpha.utils.terminal import colored

        return "%*s%s:\n" % (
            self.current_indent,
            "",
            colored(heading, fg="blue", bold=True),
        )

    def format_option(self, option):
        import textwrap
        from LiuXin_alpha.utils.terminal import colored

        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", colored(opts, fg="green"))
            indent_first = self.help_position
        else:  # start help on same line as opts
            opts = "%*s%-*s  " % (
                self.current_indent,
                "",
                opt_width + len(colored("", fg="green")),
                colored(opts, fg="green"),
            )
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option).split("\n")
            help_lines = []

            for line in help_text:
                help_lines.extend(textwrap.wrap(line, self.help_width))
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line) for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result) + "\n"


class OptionParser(optparse.OptionParser):
    def __init__(
        self,
        usage="%prog [options] filename",
        version=None,
        epilog=None,
        gui_mode=False,
        conflict_handler="resolve",
        **kwds
    ):
        import textwrap
        from LiuXin_alpha.utils.terminal import colored

        usage = textwrap.dedent(usage)
        if epilog is None:
            epilog = _("Created by ") + colored(__author__, fg="cyan")
        usage += (
            "\n\n"
            + _(
                """Whenever you pass arguments to %prog that have spaces in them, """
                '''enclose the arguments in quotation marks. For example "C:\\some path with spaces"'''
            )
            + "\n"
        )
        if version is None:
            version = "%%prog (%s %s)" % (__appname__, get_version())
        optparse.OptionParser.__init__(
            self,
            usage=usage,
            version=version,
            epilog=epilog,
            formatter=CustomHelpFormatter(),
            conflict_handler=conflict_handler,
            **kwds
        )
        self.gui_mode = gui_mode
        if False:
            # Translatable string from optparse
            _("Options")
            _("show this help message and exit")
            _("show program's version number and exit")

    def print_usage(self, file=None):
        from LiuXin_alpha.utils.terminal import ANSIStream

        s = ANSIStream(file)
        optparse.OptionParser.print_usage(self, file=s)

    def print_help(self, file=None):
        from LiuXin_alpha.utils.terminal import ANSIStream

        s = ANSIStream(file)
        optparse.OptionParser.print_help(self, file=s)

    def print_version(self, file=None):
        from LiuXin_alpha.utils.terminal import ANSIStream

        s = ANSIStream(file)
        optparse.OptionParser.print_version(self, file=s)

    def error(self, msg):
        if self.gui_mode:
            raise Exception(msg)
        optparse.OptionParser.error(self, msg)

    def merge(self, parser):
        """
        Add options from parser to self. In case of conflicts, conflicting options from
        parser are skipped.
        """
        opts = list(parser.option_list)
        groups = list(parser.option_groups)

        def merge_options(options, container):
            for opt in deepcopy(options):
                if not self.has_option(opt.get_opt_string()):
                    container.add_option(opt)

        merge_options(opts, self)

        for group in groups:
            g = self.add_option_group(group.title)
            merge_options(group.option_list, g)

    def subsume(self, group_name, msg=""):
        """
        Move all existing options into a subgroup named
        C{group_name} with description C{msg}.
        """
        opts = [opt for opt in self.options_iter() if opt.get_opt_string() not in ("--version", "--help")]
        self.option_groups = []
        subgroup = self.add_option_group(group_name, msg)
        for opt in opts:
            self.remove_option(opt.get_opt_string())
            subgroup.add_option(opt)

    def options_iter(self):
        for opt in self.option_list:
            if str(opt).strip():
                yield opt
        for gr in self.option_groups:
            for opt in gr.option_list:
                if str(opt).strip():
                    yield opt

    def option_by_dest(self, dest):
        for opt in self.options_iter():
            if opt.dest == dest:
                return opt

    def merge_options(self, lower, upper):
        """
        Merge options in lower and upper option lists into upper.
        Default values in upper are overridden by
        non default values in lower.
        """
        for dest in lower.__dict__.keys():
            if dest not in upper.__dict__:
                continue
            opt = self.option_by_dest(dest)
            if lower.__dict__[dest] != opt.default and upper.__dict__[dest] == opt.default:
                upper.__dict__[dest] = lower.__dict__[dest]

    def add_option_group(self, *args, **kwargs):
        # stdlib optparse expects the group title to be a str
        if args and isinstance(args[0], (str, bytes)):
            args = list(args)
            args[0] = str(args[0])
        return optparse.OptionParser.add_option_group(self, *args, **kwargs)

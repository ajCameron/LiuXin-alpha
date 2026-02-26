"""
Front end for the builtin conversion plugins.
"""

from LiuXin_alpha.utils.logging import default_log

# ----------------------------------------------------------------------------------------------------------------------
#
# - INPUT PLUGINS START HERE

input_plugins = []

# Done like this instead of breaking down into a loader function as this way allows for automated refactoring to work
try:
    from LiuXin_alpha.file_formats.conversion.plugins.azw4_input import AZW4Input
except Exception as e:
    debug_str = "Input conversion plugin couldn't be loaded - {}".format("AZW4Input")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("AZW4Input")
    default_log.info(info_str)
    input_plugins += [AZW4Input]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.chm_input import CHMInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("CHMInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("CHMInput")
    default_log.info(info_str)
    input_plugins += [CHMInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.comic_input import ComicInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("ComicInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("ComicInput")
    default_log.info(info_str)
    input_plugins += [ComicInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.djvu_input import DJVUInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("DJVUInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("DJVUInput")
    default_log.info(info_str)
    input_plugins += [DJVUInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.docx_input import DOCXInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("DOCXInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("DOCXInput")
    default_log.info(info_str)
    input_plugins += [DOCXInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("EPUBInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("EPUBInput")
    default_log.info(info_str)
    input_plugins += [EPUBInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("FB2Input")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("FB2Input")
    default_log.info(info_str)
    input_plugins += [FB2Input]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.html_input import HTMLInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("HTMLInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("HTMLInput")
    default_log.info(info_str)
    input_plugins += [HTMLInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("HTMLZInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("HTMLZInput")
    default_log.info(info_str)
    input_plugins += [HTMLZInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.lit_input import LITInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("LITInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("LITInput")
    default_log.info(info_str)
    input_plugins += [LITInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.lrf_input import LRFInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("LRFInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("LRFInput")
    default_log.info(info_str)
    input_plugins += [LITInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.mobi_input import MOBIInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("MOBIInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("MOBIInput")
    default_log.info(info_str)
    input_plugins += [MOBIInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.odt_input import ODTInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("ODTInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("ODTInput")
    default_log.info(info_str)
    input_plugins += [ODTInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.pdb_input import PDBInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("PDBInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("PDBInput")
    default_log.info(info_str)
    input_plugins += [PDBInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_input import PDFInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("PDFInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("PDFInput")
    default_log.info(info_str)
    input_plugins += [PDFInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.pml_input import PMLInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("PMLInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("PMLInput")
    default_log.info(info_str)
    input_plugins += [PMLInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.rb_input import RBInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("RBInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("RBInput")
    default_log.info(info_str)
    input_plugins += [RBInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.recipe_input import RecipeInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("RecipeInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("RecipeInput")
    default_log.info(info_str)
    input_plugins += [RecipeInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.rtf_input import RTFInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("RTFInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("RTFInput")
    default_log.info(info_str)
    input_plugins += [RTFInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.snb_input import SNBInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("SNBInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("SNBInput")
    default_log.info(info_str)
    input_plugins += [SNBInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.tcr_input import TCRInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("TCRInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("TCRInput")
    default_log.info(info_str)
    input_plugins += [TCRInput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.txt_input import TXTInput
except Exception as e:
    debug_str = "Conversion plugins couldn't be loaded - {}".format("TXTInput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Input conversion plugin was loaded successfully - {}".format("TXTInput")
    default_log.info(info_str)
    input_plugins += [TXTInput]


def get_input_plugins():
    """
    Returns all the loaded and active input plugins.

    :return:
    """
    return input_plugins


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
#
# - OUTPUT PLUGINS START HERE


output_plugins = []

try:
    from LiuXin_alpha.file_formats.conversion.plugins.epub_output import EPUBOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("EPUBOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("EPUBOutput")
    default_log.info(info_str)
    output_plugins += [EPUBOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_output import FB2Output
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("FB2Output")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("FB2Output")
    default_log.info(info_str)
    output_plugins += [FB2Output]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.lit_output import LITOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("LITOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("LITOutput")
    default_log.info(info_str)
    output_plugins += [LITOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.lrf_output import LRFOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("LRFOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("LRFOutput")
    default_log.info(info_str)
    output_plugins += [LRFOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.mobi_output import (
        MOBIOutput,
        AZW3Output,
    )
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("MOBIOutput & AZW3Output")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("MOBIOutput & AZW3Output")
    default_log.info(info_str)
    output_plugins += [MOBIOutput, AZW3Output]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.oeb_output import OEBOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("OEBOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("OEBOutput")
    default_log.info(info_str)
    output_plugins += [OEBOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.pdb_output import PDBOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("PDBOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("PDBOutput")
    default_log.info(info_str)
    output_plugins += [PDBOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.pdf_output import PDFOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("PDFOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("PDFOutput")
    default_log.info(info_str)
    output_plugins += [PDFOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.pml_output import PMLOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("PMLOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("PMLOutput")
    default_log.info(info_str)
    output_plugins += [PMLOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.rb_output import RBOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("RBOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("RBOutput")
    default_log.info(info_str)
    output_plugins += [RBOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.rtf_output import RTFOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("RTFOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("RTFOutput")
    default_log.info(info_str)
    output_plugins += [RTFOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.tcr_output import TCROutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("TCROutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("TCROutput")
    default_log.info(info_str)
    output_plugins += [TCROutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.txt_output import TXTOutput, TXTZOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("TXTOutput & TXTZOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("TXTOutput & TXTZOutput")
    default_log.info(info_str)
    output_plugins += [TXTOutput, TXTZOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.html_output import HTMLOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("HTMLOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("HTMLOutput")
    default_log.info(info_str)
    output_plugins += [HTMLOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_output import HTMLZOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("HTMLZOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("HTMLZOutput")
    default_log.info(info_str)
    output_plugins += [HTMLZOutput]

try:
    from LiuXin_alpha.file_formats.conversion.plugins.snb_output import SNBOutput
except Exception as e:
    debug_str = "Output plugin couldn't be loaded - {}".format("SNBOutput")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Output conversion plugin was loaded successfully - {}".format("SNBOutput")
    default_log.info(info_str)
    output_plugins += [SNBOutput]


def get_output_plugins():
    """
    Return all the currently loaded output plugins.

    :return:
    """
    return output_plugins


#
# ----------------------------------------------------------------------------------------------------------------------

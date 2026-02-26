from LiuXin_alpha.utils.logging import default_log

# Order here is non-alphabetical to more closely match add order
try:
    from LiuXin_alpha.devices.hanlin.driver import HANLINV3, HANLINV5, BOOX, SPECTRA
except Exception as e:
    HANLINV3 = None
    HANLINV5 = None
    BOOX = None
    SPECTRA = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("HANLINV3, HANLINV5, BOOX, SPECTRA")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("HANLINV3, HANLINV5, BOOX, SPECTRA")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.blackberry.driver import BLACKBERRY, PLAYBOOK
except Exception as e:
    BLACKBERRY = None
    PLAYBOOK = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("BLACKBERRY, PLAYBOOK")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("BLACKBERRY, PLAYBOOK")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.cybook.driver import CYBOOK, ORIZON, MUSE
except Exception as e:
    CYBOOK = None
    ORIZON = None
    MUSE = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("CYBOOK, ORIZON, MUSE")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("CYBOOK, ORIZON, MUSE")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.eb600.driver import (
        EB600,
        COOL_ER,
        SHINEBOOK,
        TOLINO,
        POCKETBOOK360,
        GER2,
        ITALICA,
        ECLICTO,
        DBOOK,
        INVESBOOK,
        BOOQ,
        ELONEX,
        POCKETBOOK301,
        MENTOR,
        POCKETBOOK602,
        POCKETBOOK701,
        POCKETBOOK360P,
        PI2,
        POCKETBOOK622,
    )
except Exception as e:
    EB600 = None
    COOL_ER = None
    SHINEBOOK = None
    TOLINO = None
    POCKETBOOK360 = None
    GER2 = None
    ITALICA = None
    ECLICTO = None
    DBOOK = None
    INVESBOOK = None
    BOOQ = None
    ELONEX = None
    POCKETBOOK301 = None
    MENTOR = None
    POCKETBOOK602 = None
    POCKETBOOK701 = None
    POCKETBOOK360P = None
    PI2 = None
    POCKETBOOK622 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format(
        "EB600, COOL_ER, SHINEBOOK, TOLINO, POCKETBOOK360, GER2, ITALICA, ECLICTO, DBOOK, INVESBOOK, BOOQ, ELONEX, "
        "POCKETBOOK301, MENTOR, POCKETBOOK602, POCKETBOOK701, POCKETBOOK360P, PI2, POCKETBOOK622"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format(
        "EB600, COOL_ER, SHINEBOOK, TOLINO, POCKETBOOK360, GER2, ITALICA, ECLICTO, DBOOK, INVESBOOK, BOOQ, ELONEX, "
        "POCKETBOOK301, MENTOR, POCKETBOOK602, POCKETBOOK701, POCKETBOOK360P, PI2, POCKETBOOK622"
    )
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.iliad.driver import ILIAD
except Exception as e:
    ILIAD = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("ILIAD")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("ILIAD")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.irexdr.driver import IREXDR1000, IREXDR800
except Exception as e:
    IREXDR1000 = None
    IREXDR800 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("IREXDR1000, IREXDR800")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("IREXDR1000, IREXDR800")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.jetbook.driver import (
        JETBOOK,
        MIBUK,
        JETBOOK_MINI,
        JETBOOK_COLOR,
    )
except Exception as e:
    JETBOOK = None
    MIBUK = None
    JETBOOK_MINI = None
    JETBOOK_COLOR = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("JETBOOK, MIBUK, JETBOOK_MINI, JETBOOK_COLOR")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("JETBOOK, MIBUK, JETBOOK_MINI, JETBOOK_COLOR")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.kindle.driver import KINDLE, KINDLE2, KINDLE_DX, KINDLE_FIRE
except Exception as e:
    KINDLE = None
    KINDLE2 = None
    KINDLE_DX = None
    KINDLE_FIRE = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("KINDLE, KINDLE2, KINDLE_DX, KINDLE_FIRE")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("KINDLE, KINDLE2, KINDLE_DX, KINDLE_FIRE")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.apple.driver import ITUNES
except Exception as e:
    ITUNES = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("ITUNES")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("ITUNES")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.nook.driver import NOOK, NOOK_COLOR
except Exception as e:
    NOOK = None
    NOOK_COLOR = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("NOOK, NOOK_COLOR")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("NOOK, NOOK_COLOR")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.prs505.driver import PRS505
except Exception as e:
    PRS505 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("PRS505")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("PRS505")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.prst1.driver import PRST1
except Exception as e:
    PRST1 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("PRST1")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("PRST1")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.user_defined.driver import USER_DEFINED
except Exception as e:
    USER_DEFINED = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("USER_DEFINED")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("USER_DEFINED")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.android.driver import ANDROID, S60, WEBOS
except Exception as e:
    ANDROID = None
    S60 = None
    WEBOS = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("ANDROID, S60, WEBOS")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("ANDROID, S60, WEBOS")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.nokia.driver import N770, N810, E71X, E52
except Exception as e:
    N770 = None
    N810 = None
    E71X = None
    E52 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("N770, N810, E71X, E52")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("N770, N810, E71X, E52")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.eslick.driver import ESLICK, EBK52
except Exception as e:
    ESLICK = None
    EBK52 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("ESLICK, EBK52")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("ESLICK, EBK52")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.nuut2.driver import NUUT2
except Exception as e:
    NUUT2 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("NUUT2")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("NUUT2")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.iriver.driver import IRIVER_STORY
except Exception as e:
    IRIVER_STORY = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("IRIVER_STORY")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("IRIVER_STORY")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.binatone.driver import README
except Exception as e:
    README = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("README")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("README")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.hanvon.driver import (
        N516,
        EB511,
        ALEX,
        AZBOOKA,
        THEBOOK,
        LIBREAIR,
        ODYSSEY,
        KIBANO,
    )
except Exception as e:
    N516 = None
    EB511 = None
    ALEX = None
    AZBOOKA = None
    THEBOOK = None
    LIBREAIR = None
    ODYSSEY = None
    KIBANO = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format(
        "N516, EB511, ALEX, AZBOOKA, THEBOOK, LIBREAIR, ODYSSEY, KIBANO"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format(
        "N516, EB511, ALEX, AZBOOKA, THEBOOK, LIBREAIR, ODYSSEY, KIBANO"
    )
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.edge.driver import EDGE
except Exception as e:
    EDGE = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("EDGE")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("EDGE")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.teclast.driver import (
        TECLAST_K3,
        NEWSMY,
        IPAPYRUS,
        SOVOS,
        PICO,
        SUNSTECH_EB700,
        ARCHOS7O,
        STASH,
        WEXLER,
    )
except Exception as e:
    TECLAST_K3 = None
    NEWSMY = None
    IPAPYRUS = None
    SOVOS = None
    PICO = None
    SUNSTECH_EB700 = None
    ARCHOS7O = None
    STASH = None
    WEXLER = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format(
        "TECLAST_K3, NEWSMY, IPAPYRUS, SOVOS, PICO, SUNSTECH_EB700, ARCHOS7O, STASH, WEXLER"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format(
        "TECLAST_K3, NEWSMY, IPAPYRUS, SOVOS, PICO, SUNSTECH_EB700, ARCHOS7O, STASH, WEXLER"
    )
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.sne.driver import SNE
except Exception as e:
    SNE = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("SNE")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("SNE")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.misc import (
        PALMPRE,
        AVANT,
        SWEEX,
        PDNOVEL,
        GEMEI,
        VELOCITYMICRO,
        PDNOVEL_KOBO,
        LUMIREAD,
        ALURATEK_COLOR,
        TREKSTOR,
        EEEREADER,
        NEXTBOOK,
        ADAM,
        MOOVYBOOK,
        COBY,
        EX124G,
        WAYTEQ,
        WOXTER,
        POCKETBOOK626,
    )
except Exception as e:
    PALMPRE = None
    AVANT = None
    SWEEX = None
    PDNOVEL = None
    GEMEI = None
    VELOCITYMICRO = None
    PDNOVEL_KOBO = None
    LUMIREAD = None
    ALURATEK_COLOR = None
    TREKSTOR = None
    EEEREADER = None
    NEXTBOOK = None
    ADAM = None
    MOOVYBOOK = None
    COBY = None
    EX124G = None
    WAYTEQ = None
    WOXTER = None
    POCKETBOOK626 = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format(
        "PALMPRE, AVANT, SWEEX, PDNOVEL, GEMEI, VELOCITYMICRO, PDNOVEL_KOBO, LUMIREAD, ALURATEK_COLOR, TREKSTOR, "
        "EEEREADER, NEXTBOOK, ADAM, MOOVYBOOK, COBY, EX124G, WAYTEQ, WOXTER, POCKETBOOK626"
    )
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format(
        "PALMPRE, AVANT, SWEEX, PDNOVEL, GEMEI, VELOCITYMICRO, PDNOVEL_KOBO, LUMIREAD, ALURATEK_COLOR, TREKSTOR, "
        "EEEREADER, NEXTBOOK, ADAM, MOOVYBOOK, COBY, EX124G, WAYTEQ, WOXTER, POCKETBOOK626"
    )
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.folder_device.driver import FOLDER_DEVICE_FOR_CONFIG
except Exception as e:
    FOLDER_DEVICE_FOR_CONFIG = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("FOLDER_DEVICE_FOR_CONFIG")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("FOLDER_DEVICE_FOR_CONFIG")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.kobo.driver import KOBO, KOBOTOUCH
except Exception as e:
    KOBO = None
    KOBOTOUCH = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("KOBO, KOBOTOUCH")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("KOBO, KOBOTOUCH")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.bambook.driver import BAMBOOK
except Exception as e:
    BAMBOOK = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("BAMBOOK")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("BAMBOOK")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.boeye.driver import BOEYE_BEX, BOEYE_BDX
except Exception as e:
    BOEYE_BEX = None
    BOEYE_BDX = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("BOEYE_BEX, BOEYE_BDX")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("BOEYE_BEX, BOEYE_BDX")
    default_log.info(info_str)

try:
    from LiuXin_alpha.devices.smart_device_app.driver import SMART_DEVICE_APP
except Exception as e:
    SMART_DEVICE_APP = None
    debug_str = "Device driver plugin couldn't be loaded - {}".format("SMART_DEVICE_APP")
    default_log.log_exception(debug_str, e, "DEBUG")
else:
    info_str = "Device driver plugin was loaded successfully - {}".format("SMART_DEVICE_APP")
    default_log.info(info_str)

from LiuXin_alpha.devices.mtp.driver import MTP_DEVICE

plugins = []

# Order here matters. The first matched device is the one used.
plugins += [
    HANLINV3,
    HANLINV5,
    BLACKBERRY,
    PLAYBOOK,
    CYBOOK,
    ORIZON,
    MUSE,
    ILIAD,
    IREXDR1000,
    IREXDR800,
    JETBOOK,
    JETBOOK_MINI,
    MIBUK,
    JETBOOK_COLOR,
    SHINEBOOK,
    POCKETBOOK360,
    POCKETBOOK301,
    POCKETBOOK602,
    POCKETBOOK701,
    POCKETBOOK360P,
    POCKETBOOK622,
    PI2,
    KINDLE,
    KINDLE2,
    KINDLE_DX,
    KINDLE_FIRE,
    NOOK,
    NOOK_COLOR,
    PRS505,
    PRST1,
    ANDROID,
    S60,
    WEBOS,
    N770,
    E71X,
    E52,
    N810,
    COOL_ER,
    ESLICK,
    EBK52,
    NUUT2,
    IRIVER_STORY,
    GER2,
    ITALICA,
    ECLICTO,
    DBOOK,
    INVESBOOK,
    BOOX,
    BOOQ,
    EB600,
    TOLINO,
    README,
    N516,
    KIBANO,
    THEBOOK,
    LIBREAIR,
    EB511,
    ELONEX,
    TECLAST_K3,
    NEWSMY,
    PICO,
    SUNSTECH_EB700,
    ARCHOS7O,
    SOVOS,
    STASH,
    WEXLER,
    IPAPYRUS,
    EDGE,
    SNE,
    ALEX,
    ODYSSEY,
    PALMPRE,
    KOBO,
    KOBOTOUCH,
    AZBOOKA,
    FOLDER_DEVICE_FOR_CONFIG,
    AVANT,
    MENTOR,
    SWEEX,
    PDNOVEL,
    SPECTRA,
    GEMEI,
    VELOCITYMICRO,
    PDNOVEL_KOBO,
    LUMIREAD,
    ALURATEK_COLOR,
    BAMBOOK,
    TREKSTOR,
    EEEREADER,
    NEXTBOOK,
    ADAM,
    MOOVYBOOK,
    COBY,
    EX124G,
    WAYTEQ,
    WOXTER,
    POCKETBOOK626,
    ITUNES,
    BOEYE_BEX,
    BOEYE_BDX,
    MTP_DEVICE,
    SMART_DEVICE_APP,
    USER_DEFINED,
]

plugins = [_ for _ in plugins if _ is not None]


def get_device_driver_plugins():
    """
    Return all the loaded device drivers.

    :return:
    """
    return plugins

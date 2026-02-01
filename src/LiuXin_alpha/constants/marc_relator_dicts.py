# Auto-generated from a raw MARC relator/role dump.
# Contains:
#   - MARC_REKEY_REGEX: regex-string -> 3-letter MARC relator code
#   - MARC_ROLE_DESC:  3-letter MARC relator code -> definition/description text
#
# Notes:
#   - Regex strings include an inline (?i) for case-insensitive matching.
#   - Dict insertion order is meaningful if you iterate to find the first match.

MARC_REKEY_REGEX = {'(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+forewords?\\b': 'wfw',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+introductions?\\b': 'win',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+prefaces?\\b': 'wpr',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+afterwords?\\b': 'waw',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+postfaces?\\b': 'waw',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+colophons?\\b': 'waw',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+screenplays?\\b': 'aus',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+dialogues?\\b': 'aud',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+dialogs?\\b': 'aud',
 '(?i)\\bauthours?\\b': 'aut',
 '(?i)\\bauthers?\\b': 'aut',
 '(?i)\\bautors?\\b': 'aut',
 '(?i)\\bwriters?\\b': 'aut',
 '(?i)\\bnovelists?\\b': 'aut',
 '(?i)\\bpoets?\\b': 'aut',
 '(?i)\\bessayists?\\b': 'aut',
 '(?i)\\beditors?\\b': 'edt',
 '(?i)\\bediters?\\b': 'edt',
 '(?i)\\beditiors?\\b': 'edt',
 '(?i)\\bproof[\\s_\\-/,]+readers?\\b': 'pfr',
 '(?i)\\bproofreaders?\\b': 'pfr',
 '(?i)\\billustraters?\\b': 'ill',
 '(?i)\\bilustrators?\\b': 'ill',
 '(?i)\\billustators?\\b': 'ill',
 '(?i)\\btranslators?\\b': 'trl',
 '(?i)\\btranslaters?\\b': 'trl',
 '(?i)\\btranlators?\\b': 'trl',
 '(?i)\\bnarrators?\\b': 'nrt',
 '(?i)\\bnarators?\\b': 'nrt',
 '(?i)\\bphotograhers?\\b': 'pht',
 '(?i)\\bphotographers?\\b': 'pht',
 '(?i)\\bcinematograhers?\\b': 'cng',
 '(?i)\\bcinamatographers?\\b': 'cng',
 '(?i)\\bchoreograhers?\\b': 'chr',
 '(?i)\\bcomposers?\\b': 'cmp',
 '(?i)\\bcomposors?\\b': 'cmp',
 '(?i)\\bconductors?\\b': 'cnd',
 '(?i)\\bconducters?\\b': 'cnd',
 '(?i)\\bpublishers?\\b': 'pbl',
 '(?i)\\bpublisers?\\b': 'pbl',
 '(?i)\\bpubishers?\\b': 'pbl',
 '(?i)\\bscreenwriters?\\b': 'aus',
 '(?i)\\bscreen[\\s_\\-/,]+play[\\s_\\-/,]+writers?\\b': 'aus',
 '(?i)\\bvoiceovers?\\b': 'nrt',
 '(?i)\\bvoice[\\s_\\-/,]+actors?\\b': 'vac',
 '(?i)\\bvoice[\\s_\\-/,]+actress\\b': 'vac',
 '(?i)\\bdjs?\\b': 'djo',
 '(?i)\\bd[\\s_\\-/,]+js?\\b': 'djo',
 '(?i)\\bsound[\\s_\\-/,]+engineers?\\b': 'sde',
 '(?i)\\baudio[\\s_\\-/,]+engineers?\\b': 'aue',
 '(?i)\\bsound[\\s_\\-/,]+designers?\\b': 'sds',
 '(?i)\\bbook[\\s_\\-/,]+cover[\\s_\\-/,]+designers?\\b': 'cov',
 '(?i)\\bcover[\\s_\\-/,]+arts?\\b': 'cov',
 '(?i)\\bbook[\\s_\\-/,]+designers?\\b': 'bkd',
 '(?i)\\btypesetters?\\b': 'cmt',
 '(?i)\\btypographers?\\b': 'tyg',
 '(?i)\\btypeface[\\s_\\-/,]+designers?\\b': 'tyd',
 '(?i)\\btype[\\s_\\-/,]+designers?\\b': 'tyd',
 '(?i)\\blibrarys?\\b': 'lbr',
 '(?i)\\bresearchers?\\b': 'res',
 '(?i)\\breasearchers?\\b': 'res',
 '(?i)\\bresearch[\\s_\\-/,]+team[\\s_\\-/,]+heads?\\b': 'rth',
 '(?i)\\bcurators?\\b': 'cur',
 '(?i)\\bsculptors?\\b': 'scl',
 '(?i)\\bsculpors?\\b': 'scl',
 '(?i)\\bengravers?\\b': 'egr',
 '(?i)\\betchers?\\b': 'etr',
 '(?i)\\bcartographers?\\b': 'ctg',
 '(?i)\\bmap[\\s_\\-/,]+makers?\\b': 'ctg',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+supplementary[\\s_\\-/,]+textual[\\s_\\-/,]+contents?\\b': 'wst',
 '(?i)\\bauthor[\\s_\\-/,]+in[\\s_\\-/,]+quotations[\\s_\\-/,]+or[\\s_\\-/,]+text[\\s_\\-/,]+abstracts\\b': 'aqt',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+afterword[\\s_\\-/,]+colophon[\\s_\\-/,]+etcs?\\b': 'waw',
 '(?i)\\bgeographic[\\s_\\-/,]+information[\\s_\\-/,]+specialists?\\b': 'gis',
 '(?i)\\bgeospatial[\\s_\\-/,]+information[\\s_\\-/,]+specialists?\\b': 'gis',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+accompanying[\\s_\\-/,]+materials?\\b': 'wam',
 '(?i)\\bcommentator[\\s_\\-/,]+for[\\s_\\-/,]+written[\\s_\\-/,]+texts?\\b': 'cwt',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+introduction[\\s_\\-/,]+etcs?\\b': 'wst',
 '(?i)\\bdegree[\\s_\\-/,]+granting[\\s_\\-/,]+institutions?\\b': 'dgg',
 '(?i)\\beditor[\\s_\\-/,]+of[\\s_\\-/,]+moving[\\s_\\-/,]+image[\\s_\\-/,]+works?\\b': 'edm',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+added[\\s_\\-/,]+commentarys?\\b': 'wac',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+television[\\s_\\-/,]+storys?\\b': 'wts',
 '(?i)\\bauthor[\\s_\\-/,]+of[\\s_\\-/,]+screenplay[\\s_\\-/,]+etcs?\\b': 'aus',
 '(?i)\\bbibliographic[\\s_\\-/,]+antecedents?\\b': 'ant',
 '(?i)\\bcurator[\\s_\\-/,]+of[\\s_\\-/,]+an[\\s_\\-/,]+exhibitions?\\b': 'cur',
 '(?i)\\bmoving[\\s_\\-/,]+image[\\s_\\-/,]+work[\\s_\\-/,]+editors?\\b': 'edm',
 '(?i)\\bplaintiff[\\s_\\-/,]+corporate[\\s_\\-/,]+bodys?\\b': 'ptf',
 '(?i)\\bspecial[\\s_\\-/,]+effects[\\s_\\-/,]+providers?\\b': 'sfx',
 '(?i)\\bconsultant[\\s_\\-/,]+to[\\s_\\-/,]+a[\\s_\\-/,]+projects?\\b': 'csp',
 '(?i)\\bdegree[\\s_\\-/,]+committee[\\s_\\-/,]+members?\\b': 'dgc',
 '(?i)\\bdirector[\\s_\\-/,]+of[\\s_\\-/,]+photographys?\\b': 'cng',
 '(?i)\\bvisual[\\s_\\-/,]+effects[\\s_\\-/,]+providers?\\b': 'vfx',
 '(?i)\\bdesigner[\\s_\\-/,]+of[\\s_\\-/,]+bookjackets?\\b': 'bjd',
 '(?i)\\binstrumental[\\s_\\-/,]+conductors?\\b': 'cnd',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+added[\\s_\\-/,]+lyrics\\b': 'wal',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+introductions?\\b': 'win',
 '(?i)\\bcomplainant[\\s_\\-/,]+appellants?\\b': 'cpt',
 '(?i)\\beditor[\\s_\\-/,]+of[\\s_\\-/,]+compilations?\\b': 'edc',
 '(?i)\\benacting[\\s_\\-/,]+jurisdictions?\\b': 'enj',
 '(?i)\\bjurisdiction[\\s_\\-/,]+governeds?\\b': 'jug',
 '(?i)\\bmotion[\\s_\\-/,]+picture[\\s_\\-/,]+editors?\\b': 'flm',
 '(?i)\\bperformer[\\s_\\-/,]+of[\\s_\\-/,]+researchs?\\b': 'res',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+intertitles\\b': 'wft',
 '(?i)\\bcollection[\\s_\\-/,]+registrars?\\b': 'cor',
 '(?i)\\bcomplainant[\\s_\\-/,]+appellees?\\b': 'cpe',
 '(?i)\\bcontestant[\\s_\\-/,]+appellants?\\b': 'cot',
 '(?i)\\bonscreen[\\s_\\-/,]+participants?\\b': 'onp',
 '(?i)\\borganizer[\\s_\\-/,]+of[\\s_\\-/,]+meetings?\\b': 'orm',
 '(?i)\\bproduction[\\s_\\-/,]+personnels?\\b': 'prd',
 '(?i)\\bresearch[\\s_\\-/,]+team[\\s_\\-/,]+members?\\b': 'rtm',
 '(?i)\\brespondent[\\s_\\-/,]+appellants?\\b': 'rst',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+added[\\s_\\-/,]+texts?\\b': 'wat',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+film[\\s_\\-/,]+storys?\\b': 'wfs',
 '(?i)\\bbookjacket[\\s_\\-/,]+designers?\\b': 'bjd',
 '(?i)\\bcontestant[\\s_\\-/,]+appellees?\\b': 'coe',
 '(?i)\\bcontestee[\\s_\\-/,]+appellants?\\b': 'ctt',
 '(?i)\\bdefendant[\\s_\\-/,]+appellants?\\b': 'dft',
 '(?i)\\bdesigner[\\s_\\-/,]+of[\\s_\\-/,]+bindings?\\b': 'bdd',
 '(?i)\\blaboratory[\\s_\\-/,]+directors?\\b': 'ldr',
 '(?i)\\blandscape[\\s_\\-/,]+architects?\\b': 'lsa',
 '(?i)\\bplaintiff[\\s_\\-/,]+appellants?\\b': 'ptt',
 '(?i)\\bproduction[\\s_\\-/,]+designers?\\b': 'prs',
 '(?i)\\bpublishing[\\s_\\-/,]+directors?\\b': 'pbd',
 '(?i)\\bresearch[\\s_\\-/,]+supervisors?\\b': 'rth',
 '(?i)\\brespondent[\\s_\\-/,]+appellees?\\b': 'rse',
 '(?i)\\btechnical[\\s_\\-/,]+draftsmans?\\b': 'drm',
 '(?i)\\btelevision[\\s_\\-/,]+directors?\\b': 'tld',
 '(?i)\\btelevision[\\s_\\-/,]+producers?\\b': 'tlp',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+afterwords?\\b': 'waw',
 '(?i)\\bbookplate[\\s_\\-/,]+designers?\\b': 'bpd',
 '(?i)\\bcommissioning[\\s_\\-/,]+bodys?\\b': 'pat',
 '(?i)\\bcontestee[\\s_\\-/,]+appellees?\\b': 'cte',
 '(?i)\\bcopyright[\\s_\\-/,]+claimants?\\b': 'cpc',
 '(?i)\\bcriminal[\\s_\\-/,]+defendants?\\b': 'dfd',
 '(?i)\\bdefendant[\\s_\\-/,]+appellees?\\b': 'dfe',
 '(?i)\\bdesigner[\\s_\\-/,]+of[\\s_\\-/,]+e[\\s_\\-/,]+books?\\b': 'bkd',
 '(?i)\\bdistribution[\\s_\\-/,]+places?\\b': 'dbp',
 '(?i)\\beditorial[\\s_\\-/,]+directors?\\b': 'edd',
 '(?i)\\bgraphic[\\s_\\-/,]+technicians?\\b': 'art',
 '(?i)\\blibelant[\\s_\\-/,]+appellants?\\b': 'lit',
 '(?i)\\bmaster[\\s_\\-/,]+electricians?\\b': 'elg',
 '(?i)\\bonscreen[\\s_\\-/,]+presenters?\\b': 'osp',
 '(?i)\\bplaintiff[\\s_\\-/,]+appellees?\\b': 'pte',
 '(?i)\\bplates[\\s_\\-/,]+printer[\\s_\\-/,]+ofs?\\b': 'pop',
 '(?i)\\bproduction[\\s_\\-/,]+companys?\\b': 'prn',
 '(?i)\\bproduction[\\s_\\-/,]+managers?\\b': 'pmn',
 '(?i)\\bproject[\\s_\\-/,]+supervisors?\\b': 'pdr',
 '(?i)\\brecording[\\s_\\-/,]+engineers?\\b': 'rce',
 '(?i)\\bscientific[\\s_\\-/,]+advisors?\\b': 'sad',
 '(?i)\\bsoftware[\\s_\\-/,]+developers?\\b': 'swd',
 '(?i)\\btechnical[\\s_\\-/,]+directors?\\b': 'tcd',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+forewords?\\b': 'wfw',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+postfaces?\\b': 'waw',
 '(?i)\\barranger[\\s_\\-/,]+of[\\s_\\-/,]+musics?\\b': 'arr',
 '(?i)\\bartistic[\\s_\\-/,]+directors?\\b': 'ard',
 '(?i)\\bchief[\\s_\\-/,]+electricians?\\b': 'elg',
 '(?i)\\bdedicatee[\\s_\\-/,]+of[\\s_\\-/,]+items?\\b': 'dte',
 '(?i)\\bdegree[\\s_\\-/,]+supervisors?\\b': 'dgs',
 '(?i)\\bdesigner[\\s_\\-/,]+of[\\s_\\-/,]+covers?\\b': 'cov',
 '(?i)\\bhouse[\\s_\\-/,]+electricians?\\b': 'elg',
 '(?i)\\blibelant[\\s_\\-/,]+appellees?\\b': 'lie',
 '(?i)\\blibelee[\\s_\\-/,]+appellants?\\b': 'let',
 '(?i)\\blighting[\\s_\\-/,]+designers?\\b': 'lgd',
 '(?i)\\bmanufacture[\\s_\\-/,]+places?\\b': 'mfp',
 '(?i)\\bpermitting[\\s_\\-/,]+agencys?\\b': 'pma',
 '(?i)\\bprinter[\\s_\\-/,]+of[\\s_\\-/,]+plates\\b': 'pop',
 '(?i)\\bpublication[\\s_\\-/,]+places?\\b': 'pup',
 '(?i)\\bresponsible[\\s_\\-/,]+partys?\\b': 'rpy',
 '(?i)\\btechnical[\\s_\\-/,]+advisors?\\b': 'tad',
 '(?i)\\btelevision[\\s_\\-/,]+writers?\\b': 'tau',
 '(?i)\\bwriter[\\s_\\-/,]+of[\\s_\\-/,]+prefaces?\\b': 'wpr',
 '(?i)\\bbinding[\\s_\\-/,]+designers?\\b': 'bdd',
 '(?i)\\bbraille[\\s_\\-/,]+embossers?\\b': 'brl',
 '(?i)\\bcasting[\\s_\\-/,]+directors?\\b': 'cad',
 '(?i)\\bchoral[\\s_\\-/,]+conductors?\\b': 'cnd',
 '(?i)\\bcopyright[\\s_\\-/,]+holders?\\b': 'cph',
 '(?i)\\bcostume[\\s_\\-/,]+designers?\\b': 'cst',
 '(?i)\\bdata[\\s_\\-/,]+contributors?\\b': 'dtc',
 '(?i)\\bdesigner[\\s_\\-/,]+of[\\s_\\-/,]+books?\\b': 'bkd',
 '(?i)\\bdesigner[\\s_\\-/,]+of[\\s_\\-/,]+types?\\b': 'tyd',
 '(?i)\\bdubbing[\\s_\\-/,]+directors?\\b': 'dbd',
 '(?i)\\bfilm[\\s_\\-/,]+distributors?\\b': 'fds',
 '(?i)\\bhonouree[\\s_\\-/,]+of[\\s_\\-/,]+items?\\b': 'hnr',
 '(?i)\\bhost[\\s_\\-/,]+institutions?\\b': 'his',
 '(?i)\\bhost[\\s_\\-/,]+supportings?\\b': 'sht',
 '(?i)\\blibelee[\\s_\\-/,]+appellees?\\b': 'lee',
 '(?i)\\bmetadata[\\s_\\-/,]+contacts?\\b': 'mdc',
 '(?i)\\bmusic[\\s_\\-/,]+programmers?\\b': 'mup',
 '(?i)\\bmusical[\\s_\\-/,]+directors?\\b': 'msd',
 '(?i)\\bpatent[\\s_\\-/,]+applicants?\\b': 'pta',
 '(?i)\\bplace[\\s_\\-/,]+of[\\s_\\-/,]+address\\b': 'pad',
 '(?i)\\bplaintiff[\\s_\\-/,]+persons?\\b': 'ptf',
 '(?i)\\bproducer[\\s_\\-/,]+of[\\s_\\-/,]+books?\\b': 'bkp',
 '(?i)\\bproduction[\\s_\\-/,]+places?\\b': 'prp',
 '(?i)\\bproject[\\s_\\-/,]+directors?\\b': 'pdr',
 '(?i)\\btelevision[\\s_\\-/,]+guests?\\b': 'tlg',
 '(?i)\\buniversity[\\s_\\-/,]+places?\\b': 'uvp',
 '(?i)\\bassociated[\\s_\\-/,]+names?\\b': 'asn',
 '(?i)\\battributed[\\s_\\-/,]+names?\\b': 'att',
 '(?i)\\bcamera[\\s_\\-/,]+operators?\\b': 'cop',
 '(?i)\\bcinematographers?\\b': 'cng',
 '(?i)\\bcivil[\\s_\\-/,]+defendants?\\b': 'dfd',
 '(?i)\\binstrumentalists?\\b': 'itr',
 '(?i)\\bmixing[\\s_\\-/,]+engineers?\\b': 'mxe',
 '(?i)\\bpatent[\\s_\\-/,]+inventors?\\b': 'inv',
 '(?i)\\bpreservationists?\\b': 'con',
 '(?i)\\bprocess[\\s_\\-/,]+contacts?\\b': 'prc',
 '(?i)\\bsponsoring[\\s_\\-/,]+bodys?\\b': 'spn',
 '(?i)\\bsupporting[\\s_\\-/,]+hosts?\\b': 'sht',
 '(?i)\\btelevision[\\s_\\-/,]+hosts?\\b': 'tlh',
 '(?i)\\baudio[\\s_\\-/,]+producers?\\b': 'aup',
 '(?i)\\bcourt[\\s_\\-/,]+governeds?\\b': 'cou',
 '(?i)\\bcourt[\\s_\\-/,]+reporters?\\b': 'crt',
 '(?i)\\bcover[\\s_\\-/,]+designers?\\b': 'cov',
 '(?i)\\bdegree[\\s_\\-/,]+grantors?\\b': 'dgg',
 '(?i)\\bdubious[\\s_\\-/,]+authors?\\b': 'dub',
 '(?i)\\bfield[\\s_\\-/,]+directors?\\b': 'fld',
 '(?i)\\bgame[\\s_\\-/,]+developers?\\b': 'gdv',
 '(?i)\\bmetal[\\s_\\-/,]+engravers?\\b': 'mte',
 '(?i)\\bradio[\\s_\\-/,]+directors?\\b': 'rdd',
 '(?i)\\bradio[\\s_\\-/,]+producers?\\b': 'rpc',
 '(?i)\\brestorationists?\\b': 'rsr',
 '(?i)\\bsponsored[\\s_\\-/,]+works?\\b': 'spn',
 '(?i)\\bstage[\\s_\\-/,]+directors?\\b': 'sgd',
 '(?i)\\bstandards[\\s_\\-/,]+bodys?\\b': 'stn',
 '(?i)\\bthesis[\\s_\\-/,]+advisors?\\b': 'ths',
 '(?i)\\bbook[\\s_\\-/,]+producers?\\b': 'bkp',
 '(?i)\\bchoreographers?\\b': 'chr',
 '(?i)\\bcorrespondents?\\b': 'crp',
 '(?i)\\bcounterfeiters?\\b': 'frg',
 '(?i)\\bcurrent[\\s_\\-/,]+owners?\\b': 'own',
 '(?i)\\bfilm[\\s_\\-/,]+directors?\\b': 'fmd',
 '(?i)\\bfilm[\\s_\\-/,]+producers?\\b': 'fmp',
 '(?i)\\bmakeup[\\s_\\-/,]+artists?\\b': 'mka',
 '(?i)\\bmarkup[\\s_\\-/,]+editors?\\b': 'mrk',
 '(?i)\\bmusic[\\s_\\-/,]+copyists?\\b': 'mcp',
 '(?i)\\bpatent[\\s_\\-/,]+holders?\\b': 'pth',
 '(?i)\\bstage[\\s_\\-/,]+managers?\\b': 'stm',
 '(?i)\\bsupposed[\\s_\\-/,]+names?\\b': 'att',
 '(?i)\\bwood[\\s_\\-/,]+engravers?\\b': 'wde',
 '(?i)\\bart[\\s_\\-/,]+directors?\\b': 'adi',
 '(?i)\\bblurb[\\s_\\-/,]+writers?\\b': 'blw',
 '(?i)\\bcalligraphers?\\b': 'cll',
 '(?i)\\bcollaborators?\\b': 'ctb',
 '(?i)\\bdata[\\s_\\-/,]+managers?\\b': 'dtm',
 '(?i)\\belectrotypers?\\b': 'elt',
 '(?i)\\bformer[\\s_\\-/,]+owners?\\b': 'fmo',
 '(?i)\\bissuing[\\s_\\-/,]+bodys?\\b': 'isb',
 '(?i)\\bjoint[\\s_\\-/,]+authors?\\b': 'aut',
 '(?i)\\blab[\\s_\\-/,]+directors?\\b': 'ldr',
 '(?i)\\blithographers?\\b': 'ltg',
 '(?i)\\bmanufacturers?\\b': 'mfr',
 '(?i)\\bminute[\\s_\\-/,]+takers?\\b': 'mtk',
 '(?i)\\bremix[\\s_\\-/,]+artists?\\b': 'rxa',
 '(?i)\\bsecond[\\s_\\-/,]+partys?\\b': 'spy',
 '(?i)\\bset[\\s_\\-/,]+designers?\\b': 'std',
 '(?i)\\bvideographers?\\b': 'vdg',
 '(?i)\\bart[\\s_\\-/,]+copyists?\\b': 'acp',
 '(?i)\\bautographers?\\b': 'ato',
 '(?i)\\bbook[\\s_\\-/,]+artists?\\b': 'bka',
 '(?i)\\bbowdlerizers?\\b': 'cns',
 '(?i)\\bbroadcasters?\\b': 'brd',
 '(?i)\\bcommentators?\\b': 'cmm',
 '(?i)\\bcomplainants?\\b': 'cpl',
 '(?i)\\bconservators?\\b': 'con',
 '(?i)\\bcontributors?\\b': 'ctb',
 '(?i)\\bdistributors?\\b': 'dst',
 '(?i)\\belectricians?\\b': 'elg',
 '(?i)\\bevent[\\s_\\-/,]+places?\\b': 'evp',
 '(?i)\\bfacsimilists?\\b': 'fac',
 '(?i)\\bfilm[\\s_\\-/,]+editors?\\b': 'flm',
 '(?i)\\bfirst[\\s_\\-/,]+partys?\\b': 'fpy',
 '(?i)\\billuminators?\\b': 'ilu',
 '(?i)\\billustrators?\\b': 'ill',
 '(?i)\\binterviewees?\\b': 'ive',
 '(?i)\\binterviewers?\\b': 'ivr',
 '(?i)\\bnews[\\s_\\-/,]+anchors?\\b': 'nan',
 '(?i)\\bstereotypers?\\b': 'str',
 '(?i)\\bstorytellers?\\b': 'stl',
 '(?i)\\btranscribers?\\b': 'trc',
 '(?i)\\bauctioneers?\\b': 'auc',
 '(?i)\\bbooksellers?\\b': 'bsl',
 '(?i)\\bcollotypers?\\b': 'clt',
 '(?i)\\bcompositors?\\b': 'cmt',
 '(?i)\\bconsultants?\\b': 'csl',
 '(?i)\\bcontestants?\\b': 'cos',
 '(?i)\\bcontractors?\\b': 'ctr',
 '(?i)\\bdelineators?\\b': 'dln',
 '(?i)\\bdissertants?\\b': 'dis',
 '(?i)\\bexpurgators?\\b': 'cns',
 '(?i)\\beyewitness\\b': 'wit',
 '(?i)\\bimprimaturs?\\b': 'lso',
 '(?i)\\binstructors?\\b': 'tch',
 '(?i)\\blaboratorys?\\b': 'lbr',
 '(?i)\\blibrettists?\\b': 'lbt',
 '(?i)\\boriginators?\\b': 'org',
 '(?i)\\bpapermakers?\\b': 'ppm',
 '(?i)\\bplatemakers?\\b': 'plt',
 '(?i)\\bprintmakers?\\b': 'prm',
 '(?i)\\bprogrammers?\\b': 'prg',
 '(?i)\\brapporteurs?\\b': 'rap',
 '(?i)\\brepositorys?\\b': 'rps',
 '(?i)\\brespondents?\\b': 'rsp',
 '(?i)\\brubricators?\\b': 'rbr',
 '(?i)\\bwoodcutters?\\b': 'wdc',
 '(?i)\\baddressees?\\b': 'rcp',
 '(?i)\\bannotators?\\b': 'ann',
 '(?i)\\bannouncers?\\b': 'anc',
 '(?i)\\bappellants?\\b': 'apl',
 '(?i)\\bapplicants?\\b': 'app',
 '(?i)\\bappraisers?\\b': 'exp',
 '(?i)\\barchitects?\\b': 'arc',
 '(?i)\\bcollectors?\\b': 'col',
 '(?i)\\bcolourists?\\b': 'clr',
 '(?i)\\bconceptors?\\b': 'ccp',
 '(?i)\\bcontestees?\\b': 'cts',
 '(?i)\\bcorrectors?\\b': 'crr',
 '(?i)\\bdedicatees?\\b': 'dte',
 '(?i)\\bdedicators?\\b': 'dto',
 '(?i)\\bdefendants?\\b': 'dfd',
 '(?i)\\bdepositors?\\b': 'dpt',
 '(?i)\\bdraftsmans?\\b': 'drm',
 '(?i)\\bfilmmakers?\\b': 'fmk',
 '(?i)\\binscribers?\\b': 'ins',
 '(?i)\\bmoderators?\\b': 'mod',
 '(?i)\\borganizers?\\b': 'orm',
 '(?i)\\bpencillers?\\b': 'pnc',
 '(?i)\\bperformers?\\b': 'prf',
 '(?i)\\bplaintiffs?\\b': 'ptf',
 '(?i)\\bpresenters?\\b': 'pre',
 '(?i)\\bpuppeteers?\\b': 'ppt',
 '(?i)\\brecipients?\\b': 'rcp',
 '(?i)\\brecordists?\\b': 'rcd',
 '(?i)\\bscenarists?\\b': 'sce',
 '(?i)\\bsecretarys?\\b': 'sec',
 '(?i)\\btestifiers?\\b': 'wit',
 '(?i)\\babridgers?\\b': 'abr',
 '(?i)\\banimators?\\b': 'anm',
 '(?i)\\bappellees?\\b': 'ape',
 '(?i)\\barrangers?\\b': 'arr',
 '(?i)\\bassignees?\\b': 'asg',
 '(?i)\\bcolorists?\\b': 'clr',
 '(?i)\\bcompilers?\\b': 'com',
 '(?i)\\bdepicteds?\\b': 'dpc',
 '(?i)\\bdeponents?\\b': 'wit',
 '(?i)\\bdesigners?\\b': 'dsr',
 '(?i)\\bdirectors?\\b': 'drt',
 '(?i)\\bengineers?\\b': 'eng',
 '(?i)\\bhonourees?\\b': 'hnr',
 '(?i)\\binventors?\\b': 'inv',
 '(?i)\\bletterers?\\b': 'ltr',
 '(?i)\\blibelants?\\b': 'lil',
 '(?i)\\blicensees?\\b': 'lse',
 '(?i)\\blicensors?\\b': 'lso',
 '(?i)\\blyricists?\\b': 'lyr',
 '(?i)\\bmusicians?\\b': 'mus',
 '(?i)\\bobservers?\\b': 'wit',
 '(?i)\\bonlookers?\\b': 'wit',
 '(?i)\\bopponents?\\b': 'opn',
 '(?i)\\bpanelists?\\b': 'pan',
 '(?i)\\bpatentees?\\b': 'pth',
 '(?i)\\bpencilers?\\b': 'pnc',
 '(?i)\\bproducers?\\b': 'pro',
 '(?i)\\bpromoters?\\b': 'ths',
 '(?i)\\bproviders?\\b': 'prv',
 '(?i)\\bredaktors?\\b': 'red',
 '(?i)\\brenderers?\\b': 'ren',
 '(?i)\\breporters?\\b': 'rpt',
 '(?i)\\brestagers?\\b': 'rsg',
 '(?i)\\breviewers?\\b': 'rev',
 '(?i)\\bsurveyors?\\b': 'srv',
 '(?i)\\bvocalists?\\b': 'sng',
 '(?i)\\badapters?\\b': 'adp',
 '(?i)\\banalysts?\\b': 'anl',
 '(?i)\\bcreators?\\b': 'cre',
 '(?i)\\bencoders?\\b': 'mrk',
 '(?i)\\bfounders?\\b': 'fon',
 '(?i)\\bhonorees?\\b': 'hnr',
 '(?i)\\blibelees?\\b': 'lel',
 '(?i)\\bmarblers?\\b': 'mrb',
 '(?i)\\bmonitors?\\b': 'mon',
 '(?i)\\bpraeses\\b': 'pra',
 '(?i)\\bprinters?\\b': 'prt',
 '(?i)\\bsettings?\\b': 'stg',
 '(?i)\\bspeakers?\\b': 'spk',
 '(?i)\\bsponsors?\\b': 'spn',
 '(?i)\\bteachers?\\b': 'tch',
 '(?i)\\bwitness\\b': 'wit',
 '(?i)\\bartists?\\b': 'art',
 '(?i)\\bauthors?\\b': 'aut',
 '(?i)\\bbinders?\\b': 'bnd',
 '(?i)\\bcasters?\\b': 'cas',
 '(?i)\\bcensors?\\b': 'cns',
 '(?i)\\bclients?\\b': 'cli',
 '(?i)\\bcopiers?\\b': 'fac',
 '(?i)\\bdancers?\\b': 'dnc',
 '(?i)\\bexperts?\\b': 'exp',
 '(?i)\\bforgers?\\b': 'frg',
 '(?i)\\bfunders?\\b': 'fnd',
 '(?i)\\blenders?\\b': 'len',
 '(?i)\\bmediums?\\b': 'med',
 '(?i)\\bpatrons?\\b': 'pat',
 '(?i)\\bscribes?\\b': 'scr',
 '(?i)\\bsellers?\\b': 'sll',
 '(?i)\\bsigners?\\b': 'sgn',
 '(?i)\\bsingers?\\b': 'sng',
 '(?i)\\bactors?\\b': 'act',
 '(?i)\\bdonors?\\b': 'dnr',
 '(?i)\\binkers?\\b': 'ink',
 '(?i)\\bjudges?\\b': 'jud',
 '(?i)\\bothers?\\b': 'oth',
 '(?i)\\bowners?\\b': 'own',
 '(?i)\\bhosts?\\b': 'hst',
 '(?i)\\bleads?\\b': 'led'}

MARC_ROLE_DESC = {'abr': 'A person, family, or organization contributing to a resource by shortening or condensing the original work '
        'but leaving the nature and content of the original work substantially unchanged. For substantial '
        'modifications that result in the creation of a new work, see author.',
 'acp': 'A person (e.g., a painter or sculptor) who makes copies of works of visual art.',
 'act': 'A performer contributing to an expression of a work by acting as a cast member or player in a musical or '
        'dramatic presentation, etc.',
 'adi': 'A person contributing to a motion picture or television production by overseeing the artists and craftspeople '
        'who build the sets.',
 'adp': 'A person or organization who modifies the content of existing work for a different medium or audience, as in '
        'adapting a novel for a motion picture, creating a young reader’s version of a book, or reworking a musical '
        'composition.',
 'anc': 'A person who makes announcements on television or radio to identify stations, introduce and close shows, '
        'announce station breaks, commercials, and public service information. May also read news flashes and describe '
        'other public and sporting events.',
 'anl': 'A person or organization that reviews, examines and interprets data or information in a specific area.',
 'anm': 'A person contributing to a moving image work or computer program by giving apparent movement to inanimate '
        'objects or drawings. For the creator of the drawings that are animated, see artist.',
 'ann': 'A person who makes manuscript annotations on an item',
 'ant': 'A person or organization responsible for a resource upon which the resource represented by the bibliographic '
        'description is based. This may be appropriate for adaptations, sequels, continuations, indexes, etc.',
 'ape': 'A person or organization against whom an appeal is taken.',
 'apl': "A person or organization who appeals a lower court's decision.",
 'app': 'A person or organization responsible for the submission of an application or who is named as eligible for the '
        'results of the processing of the application (e.g., bestowing of rights, reward, title, position).',
 'aqt': 'A person or organization whose work is largely quoted or extracted in works to which he or she did not '
        'contribute directly. Such quotations are found particularly in exhibition catalogs, collections of '
        'photographs, etc.',
 'arc': 'A person, family, or organization responsible for creating an architectural design, including a pictorial '
        'representation intended to show how a building, etc., will look when completed. It also oversees the '
        'construction of structures.',
 'ard': 'A person responsible for controlling the development of the artistic style of an entire production, including '
        'the choice of works to be presented and selection of senior production staff.',
 'arr': 'A person, family, or organization contributing to a musical work by rewriting the composition for a medium of '
        'performance different from that for which the work was originally intended, or modifying the work for the '
        'same medium of performance, etc., such that the musical substance of the original composition remains '
        'essentially unchanged. For extensive modification that effectively results in the creation of a new musical '
        'work, see composer.',
 'art': 'A person, family, or organization responsible for creating a work by conceiving, and implementing, an '
        'original graphic design, drawing, painting, etc. For book illustrators, prefer Illustrator [ill].',
 'asg': 'A person or organization to whom a license for printing or publishing has been transferred.',
 'asn': 'A person or organization associated with or found in an item or collection, which cannot be determined to be '
        'that of a Former owner [fmo] or other designated relationship indicative of provenance.',
 'ato': 'A person whose manuscript signature appears on an item.',
 'att': 'An author, artist, etc., relating him/her to a resource for which there is or once was substantial authority '
        'for designating that person as author, creator, etc. of the work.',
 'auc': 'A person or organization in charge of the estimation and public auctioning of goods, particularly books, '
        'artistic works, etc.',
 'aud': 'A person or organization responsible for the dialog or spoken commentary for a screenplay or sound recording.',
 'aue': 'A person or organization contributing to a resource by managing the technical aspects of sound during the '
        'processes of recording, mixing, and reproduction.',
 'aup': 'A producer responsible for most of the business aspects of an audio recording.',
 'aus': 'An author of a screenplay, script, or scene.',
 'aut': 'A person, family, or organization responsible for creating a work that is primarily textual in content, '
        'regardless of media type (e.g., printed text, spoken word, electronic text, tactile text) or genre (e.g., '
        'poems, novels, screenplays, blogs). Use also for persons, etc., creating a new work by paraphrasing, '
        'rewriting, or adapting works by another creator such that the modification has substantially changed the '
        'nature and content of the original or changed the medium of expression.',
 'bdd': 'A person or organization responsible for the binding design of a book, including the type of binding, the '
        'type of materials used, and any decorative aspects of the binding.',
 'bjd': 'A person or organization responsible for the design of flexible covers designed for or published with a book, '
        'including the type of materials used, and any decorative aspects of the bookjacket.',
 'bka': 'A person who is responsible for exploiting the book form or altering its physical structure.',
 'bkd': 'A person or organization involved in manufacturing a manifestation by being responsible for the entire '
        'graphic design of a book, including arrangement of type and illustration, choice of materials, and process '
        'used.',
 'bkp': 'A person or organization responsible for the production of books and other print media.',
 'blw': 'A person or organization responsible for writing a commendation or testimonial for a work, which appears on '
        'or within the publication itself, frequently on the back or dust jacket of print publications or on '
        'advertising material for all media.',
 'bnd': 'A person who binds an item.',
 'bpd': "A person or organization responsible for the design of a book owner's identification label that is most "
        'commonly pasted to the inside front cover of a book.',
 'brd': 'A person, family, or organization involved in broadcasting a resource to an audience via radio, television, '
        'webcast, etc.',
 'brl': 'A person, family, or organization involved in manufacturing a resource by embossing Braille cells using a '
        'stylus, special embossing printer, or other device.',
 'bsl': 'A person or organization who makes books and other bibliographic materials available for purchase. Interest '
        'in the materials is primarily lucrative.',
 'cad': 'A person responsible for most aspects of assigning roles and duties to performers.',
 'cas': 'A person, family, or organization involved in manufacturing a resource by pouring a liquid or molten '
        'substance into a mold and leaving it to solidify to take the shape of the mold.',
 'ccp': 'A person or organization responsible for the original idea on which a work is based, this includes the '
        'scientific author of an audio-visual item and the conceptor of an advertisement.',
 'chr': 'A person responsible for creating or contributing to a work of movement.',
 'cli': 'A person or organization for whom another person or organization is acting.',
 'cll': 'A person or organization who writes in an artistic hand, usually as a copyist and or engrosser.',
 'clr': 'A person or organization responsible for applying color to drawings, prints, photographs, maps, moving '
        'images, etc.',
 'clt': 'A person, family, or organization involved in manufacturing a manifestation of photographic prints from film '
        'or other colloid that has ink-receptive and ink-repellent surfaces.',
 'cmm': 'A performer contributing to a work by providing interpretation, analysis, or a discussion of the subject '
        'matter on a recording, film, or other audiovisual medium.',
 'cmp': 'A person, family, or organization responsible for creating or contributing to a musical resource by adding '
        'music to a work that originally lacked it or supplements it.',
 'cmt': 'A person or organization responsible for the creation of metal slug, or molds made of other materials, used '
        'to produce the text and images in printed matter.',
 'cnd': 'A performer contributing to a musical resource by leading a performing group (orchestra, chorus, opera, etc.) '
        'in a musical or dramatic presentation, etc.',
 'cng': 'A person in charge of photographing a motion picture, who plans the technical aspets of lighting and '
        'photographing of scenes, and often assists the director in the choice of angles, camera setups, and lighting '
        'moods. He or she may also supervise the further processing of filmed material up to the completion of the '
        'work print. Cinematographer is also referred to as director of photography. Do not confuse with videographer.',
 'cns': 'A person or organization who examines bibliographic resources for the purpose of suppressing parts deemed '
        'objectionable on moral, political, military, or other grounds.',
 'coe': 'A contestant against whom an appeal is taken from one court of law or jurisdiction to another to reverse the '
        'judgment.',
 'col': 'A curator who brings together items from various sources that are then arranged, described, and cataloged as '
        'a collection. A collector is neither the creator of the material nor a person to whom manuscripts in the '
        'collection may have been addressed.',
 'com': 'A person, family, or organization responsible for creating a new work (e.g., a bibliography, a directory) '
        'through the act of compilation, e.g., selecting, arranging, aggregating, and editing data, information, etc.',
 'con': 'A person or organization responsible for documenting, preserving, or treating printed or manuscript material, '
        'works of art, artifacts, or other media.',
 'cop': 'A person who operates a motion picture camera to film a moving image resource.',
 'cor': 'A curator who lists or inventories the items in an aggregate work such as a collection of items or works.',
 'cos': 'A person(s) or organization who opposes, resists, or disputes, in a court of law, a claim, decision, result, '
        'etc.',
 'cot': 'A contestant who takes an appeal from one court of law or jurisdiction to another to reverse the judgment.',
 'cou': 'A court governed by court rules, regardless of their official nature (e.g., laws, administrative '
        'regulations).',
 'cov': 'A person or organization responsible for the graphic design of a book cover, album cover, slipcase, box, '
        'container, etc. For a person or organization responsible for the graphic design of an entire book, use book '
        'designer; for book jackets, use bookjacket designer.',
 'cpc': 'A person or organization listed as a copyright owner at the time of registration. Copyright can be granted or '
        'later transferred to another person or organization, at which time the claimant becomes the copyright holder.',
 'cpe': 'A complainant against whom an appeal is taken from one court or jurisdiction to another to reverse the '
        'judgment, usually in an equity proceeding.',
 'cph': 'A person or organization to whom copy and legal rights have been granted or transferred for the intellectual '
        'content of a work. The copyright holder, although not necessarily the creator of the work, usually has the '
        'exclusive right to benefit financially from the sale and use of the work to which the associated copyright '
        'protection applies.',
 'cpl': 'A person or organization who applies to the courts for redress, usually in an equity proceeding.',
 'cpt': 'A complainant who takes an appeal from one court or jurisdiction to another to reverse the judgment, usually '
        'in an equity proceeding.',
 'cre': 'A person or organization responsible for the intellectual or artistic content of a resource.',
 'crp': 'A person or organization who was either the writer or recipient of a letter or other communication.',
 'crr': 'A person or organization who is a corrector of manuscripts, such as the scriptorium official who corrected '
        'the work of a scribe. For printed matter, use proofreader.',
 'crt': "A person, family, or organization contributing to a resource by preparing a court's opinions for publication.",
 'csl': 'A person or organization relevant to a resource, who is called upon for professional advice or services in a '
        'specialized field of knowledge or training.',
 'csp': 'A person or organization relevant to a resource, who is engaged specifically to provide an intellectual '
        'overview of a strategic or operational task and by analysis, specification, or instruction, to create or '
        'propose a cost-effective course of action or solution.',
 'cst': 'A person, family, or organization that designs the costumes for a moving image production or for a musical or '
        'dramatic presentation or entertainment.',
 'ctb': 'A person, family or organization responsible for making contributions to the resource. This includes those '
        'whose work has been contributed to a larger work, such as an anthology, serial publication, or other '
        'compilation of individual works. If a more specific role is available, prefer that, e.g. editor, compiler, '
        'illustrator.',
 'cte': 'A contestee against whom an appeal is taken from one court of law or jurisdiction to another to reverse the '
        'judgment.',
 'ctg': 'A person, family, or organization responsible for creating a map, atlas, globe, or other cartographic work.',
 'ctr': 'A person or organization relevant to a resource, who enters into a contract with another person or '
        'organization to perform a specific task.',
 'cts': 'A person(s) or organization defending a claim, decision, result, etc. being opposed, resisted, or disputed in '
        'a court of law.',
 'ctt': 'A contestee who takes an appeal from one court or jurisdiction to another to reverse the judgment.',
 'cur': 'A person, family, or organization conceiving, aggregating, and/or organizing an exhibition, collection, or '
        'other item.',
 'cwt': 'A person or organization responsible for the commentary or explanatory notes about a text. For the writer of '
        'manuscript annotations in a printed book, use annotator.',
 'dbd': 'A person responsible for the general management and supervision of adding new dialog or other sounds to '
        'complete a soundtrack.',
 'dbp': 'A place from which a resource, e.g., a serial, is distributed.',
 'dfd': 'A person or organization who is accused in a criminal proceeding or sued in a civil proceeding.',
 'dfe': 'A defendant against whom an appeal is taken from one court or jurisdiction to another to reverse the '
        'judgment, usually in a legal action.',
 'dft': 'A defendant who takes an appeal from one court or jurisdiction to another to reverse the judgment, usually in '
        'a legal action.',
 'dgc': 'A person who is part of a committee that considers the merit of a thesis, dissertation, or other submission '
        'by an academic degree candidate.',
 'dgg': 'A organization granting an academic degree.',
 'dgs': 'A person overseeing a higher level academic degree.',
 'dis': 'A person who presents a thesis for a university or higher-level educational degree.',
 'djo': 'A person who mixes recorded tracks together during a live performance or in a recording studio to appear as '
        'one continuous track.',
 'dln': "A person or organization executing technical drawings from others' designs.",
 'dnc': 'A performer who dances in a musical, dramatic, etc., presentation.',
 'dnr': 'A former owner of an item who donated that item to another owner.',
 'dpc': 'An entity depicted or portrayed in a work, particularly in a work of art.',
 'dpt': 'A current owner of an item who deposited the item into the custody of another person, family, or '
        'organization, while still retaining ownership.',
 'drm': 'A person, family, or organization contributing to a resource by an architect, inventor, etc., by making '
        'detailed plans or drawings for buildings, ships, aircraft, machines, objects, etc.',
 'drt': 'A person responsible for the general management and supervision of a filmed performance, a radio or '
        'television program, etc.',
 'dsr': 'A person, family, or organization responsible for creating a design for an object.',
 'dst': 'A person or organization that has exclusive or shared marketing rights for a resource.',
 'dtc': 'A person or organization that submits data for inclusion in a database or other collection of data.',
 'dte': 'A person, family, or organization to whom a resource is dedicated.',
 'dtm': 'A person or organization responsible for managing databases or other data sources.',
 'dto': 'A person who writes a dedication, which may be a formal statement or in epistolary or verse form.',
 'dub': 'A person or organization to which authorship has been dubiously or incorrectly ascribed.',
 'edc': 'A person, family, or organization contributing to a collective or aggregate work by selecting and putting '
        'together works, or parts of works, by one or more creators. For compilations of data, information, etc., that '
        'result in new works, see compiler.',
 'edd': 'A person or organization having legal and/or intellectual responsibility other than creation for the content '
        'of a serial, integrating resource, or multipart monographic work.',
 'edm': 'A person, family, or organization responsible for assembling, arranging, and trimming film, video, or other '
        'moving image formats, including both visual and audio aspects.',
 'edt': 'A person, family, or organization contributing to a resource by revising or elucidating the content, e.g., '
        'adding an introduction, notes, or other critical matter. An editor may also prepare a resource for '
        'production, publication, or distribution. For major revisions, adaptations, etc., that substantially change '
        'the nature and content of the original work, resulting in a new work, see author.',
 'egr': 'A person or organization who cuts letters, figures, etc. on a surface, such as a wooden or metal plate used '
        'for printing.',
 'elg': 'A person responsible for setting up a lighting rig and focusing the lights for a production, and running the '
        'lighting at a performance.',
 'elt': 'A person or organization who creates a duplicate printing surface by pressure molding and electrodepositing '
        'of metal that is then backed up with lead for printing.',
 'eng': 'A person or organization that is responsible for technical planning and design, particularly with '
        'construction.',
 'enj': 'A jurisdiction enacting a law, regulation, constitution, court rule, etc.',
 'etr': 'A person or organization who produces text or images for printing by subjecting metal, glass, or some other '
        'surface to acid or the corrosive action of some other substance.',
 'evp': 'A place where an event such as a conference or a concert took place.',
 'exp': 'A person or organization in charge of the description and appraisal of the value of goods, particularly rare '
        'items, works of art, etc.',
 'fac': 'A person or organization that executed the facsimile.',
 'fds': 'A person, family, or organization involved in distributing a moving image resource to theatres or other '
        'distribution channels.',
 'fld': 'A person or organization that manages or supervises the work done to collect raw data or do research in an '
        'actual setting or environment (typically applies to the natural and social sciences).',
 'flm': 'A person who, following the script and in creative cooperation with the Director, selects, arranges, and '
        'assembles the filmed material, controls the synchronization of picture and sound, and participates in other '
        'post-production tasks such as sound mixing and visual effects processing. Today, picture editing is often '
        'performed digitally.',
 'fmd': 'A director responsible for the general management and supervision of a filmed performance.',
 'fmk': 'A person, family or organization responsible for creating an independent or personal film. A filmmaker is '
        'individually responsible for the conception and execution of all aspects of the film.',
 'fmo': 'A person, family, or organization formerly having legal possession of an item.',
 'fmp': 'A producer responsible for most of the business aspects of a film.',
 'fnd': 'A person or organization that furnished financial support for the production of the work.',
 'fon': 'A person responsible for initiating a diachronic work.',
 'fpy': 'A person or organization who is identified as the only party or the party of the first party. In the case of '
        'transfer of rights, this is the assignor, transferor, licensor, grantor, etc. Multiple parties can be named '
        'jointly as the first party.',
 'frg': 'A person or organization who makes or imitates something of value or importance, especially with the intent '
        'to defraud.',
 'gdv': 'A person or organization who researches, designs, implements or tests video games, computer games, virtual '
        'reality (VR) games, etc.',
 'gis': 'A person responsible for geographic information system (GIS) development and integration with global '
        'positioning system data.',
 'his': 'An organization hosting the event, exhibit, conference, etc., which gave rise to a resource, but having '
        'little or no responsibility for the content of the resource.',
 'hnr': 'A person, family, or organization honored by a work or item (e.g., the honoree of a festschrift, a person to '
        'whom a copy is presented).',
 'hst': 'A performer contributing to a resource by leading a program (often broadcast) that includes other guests, '
        'performers, etc. (e.g., talk show host).',
 'ill': 'A person, family, or organization contributing to a resource by supplementing the primary content with '
        'drawings, diagrams, photographs, etc. If the work is primarily the artistic content created by this entity, '
        'use artist or photographer.',
 'ilu': 'A person providing decoration to a specific item using precious metals or color, often with elaborate designs '
        'and motifs.',
 'ink': 'A person or organization responsible for adding solid lines, shading and additional details to the initial '
        'pencil drawings in the creation of comic books, graphic novels, animation, etc.',
 'ins': 'A person who has written a statement of dedication or gift.',
 'inv': 'A person, family, or organization responsible for creating a new device or process.',
 'isb': 'A person, family or organization issuing a work, such as an official organ of the body.',
 'itr': 'A performer contributing to a resource by playing a musical instrument.',
 'ive': 'A person, family or organization responsible for creating or contributing to a resource by responding to an '
        'interviewer, usually a reporter, pollster, or some other information gathering agent.',
 'ivr': 'A person, family, or organization responsible for creating or contributing to a resource by acting as an '
        'interviewer, reporter, pollster, or some other information gathering agent.',
 'jud': 'A person who hears and decides on legal matters in court.',
 'jug': 'A jurisdiction governed by a law, regulation, etc., that was enacted by another jurisdiction.',
 'lbr': 'An organization that provides scientific analyses of material samples.',
 'lbt': 'An author of a libretto of an opera or other stage work, or an oratorio.',
 'ldr': 'A person or organization that manages or supervises work done in a controlled setting or environment.',
 'led': 'A person or organization that takes primary responsibility for a particular activity or endeavor. May be '
        'combined with another relator term or code to show the greater importance this person or organization has '
        'regarding that particular role. If more than one relator is assigned to a heading, use the lead relator only '
        'if it applies to all the relators.',
 'lee': 'A libelee against whom an appeal is taken from one ecclesiastical court or admiralty to another to reverse '
        'the judgment.',
 'lel': 'A person or organization against whom a libel has been filed in an ecclesiastical court or admiralty.',
 'len': 'A person or organization permitting the temporary use of a book, manuscript, etc., such as for photocopying '
        'or microfilming.',
 'let': 'A libelee who takes an appeal from one ecclesiastical court or admiralty to another to reverse the judgment.',
 'lgd': 'A person or organization who designs the lighting scheme for a theatrical presentation, entertainment, motion '
        'picture, etc.',
 'lie': 'A libelant against whom an appeal is taken from one ecclesiastical court or admiralty to another to reverse '
        'the judgment.',
 'lil': 'A person or organization who files a libel in an ecclesiastical or admiralty case.',
 'lit': 'A libelant who takes an appeal from one ecclesiastical court or admiralty to another to reverse the judgment.',
 'lsa': 'An architect responsible for creating landscape works. This work involves coordinating the arrangement of '
        'existing and proposed land features and structures.',
 'lse': 'A person or organization who is an original recipient of the right to print or publish.',
 'lso': 'A person or organization who is a signer of the license, imprimatur, etc.',
 'ltg': 'A person or organization who prepares the stone or plate for lithographic printing, including a graphic '
        'artist creating a design directly on the surface from which printing will be done.',
 'ltr': 'A person who draws text and graphic sound effects for a comic book, graphic novel, etc.',
 'lyr': 'An author of the words of a non-dramatic musical work (e.g. the text of a song), except for oratorios.',
 'mcp': 'A person who transcribes or copies musical notation.',
 'mdc': 'A person or organization primarily responsible for compiling and maintaining the original description of a '
        'metadata set (e.g., geospatial metadata set).',
 'med': 'A person held to be a channel of communication between the earthly world and a world of spirits.',
 'mfp': 'The place of manufacture (e.g., printing, duplicating, casting, etc.) of a resource in a published form.',
 'mfr': 'A person or organization responsible for printing, duplicating, casting, etc. a resource.',
 'mka': 'A person who contributes to a moving image production or for a musical or dramatic presentation by applying '
        'makeup and prosthetics.',
 'mod': 'A performer contributing to a resource by leading a program (often broadcast) where topics are discussed, '
        'usually with participation of experts in fields related to the discussion.',
 'mon': 'A person or organization that supervises compliance with the contract and is responsible for the report and '
        'controls its distribution. Sometimes referred to as the grantee, or controlling agency.',
 'mrb': 'The entity responsible for marbling paper, cloth, leather, etc. used in construction of a resource.',
 'mrk': 'A person or organization performing the coding of SGML, HTML, or XML markup of metadata, text, etc.',
 'msd': 'A person who coordinates the activities of the composer, the sound editor, and sound mixers for a moving '
        'image production or for a musical or dramatic presentation or entertainment.',
 'mte': 'An engraver responsible for decorations, illustrations, letters, etc. cut on a metal surface for printing or '
        'decoration.',
 'mtk': 'A person, family, or organization responsible for recording the minutes of a meeting.',
 'mup': 'A person who uses electronic audio devices or computer software to generate sounds.',
 'mus': 'A person or organization who performs music or contributes to the musical content of a work when it is not '
        'possible or desirable to identify the function more precisely.',
 'mxe': 'A person or organization contributing to the audio content of a resource by manipulating, mixing and '
        'assembling the tracks of an audio recording.',
 'nan': 'A person who is in overall control of the presentation of a news or current affairs television program.',
 'nrt': 'A performer contributing to a resource by reading or speaking in order to give an account of an act, '
        'occurrence, course of events, etc.',
 'onp': 'A person contributing to a nonfiction moving image work by taking an active role as a participant.',
 'opn': 'A person or organization responsible for opposing a thesis or dissertation.',
 'org': 'A person or organization performing the work, i.e., the name of a person or organization associated with the '
        'intellectual content of the work. This category does not include the publisher or personal affiliation, or '
        'sponsor except where it is also the corporate author.',
 'orm': 'A person, family, or organization organizing the exhibit, event, conference, etc., which gave rise to a '
        'resource.',
 'osp': 'A performer contributing to an expression of a work by appearing on screen in nonfiction moving image '
        'materials or introductions to fiction moving image materials to provide contextual or background information. '
        'Use when another term (e.g., narrator, host) is either not applicable or not desired.',
 'oth': 'A role that has no equivalent in the MARC list.',
 'own': 'A person, family, or organization that currently owns an item or collection, i.e.has legal possession of a '
        'resource.',
 'pad': 'The place to which a resource is sent, for example, the place of the postal address of a letter.',
 'pan': 'A performer contributing to a resource by participating in a program (often broadcast) where topics are '
        'discussed, usually with participation of experts in fields related to the discussion.',
 'pat': 'A person or organization responsible for commissioning a work. Usually a patron uses his or her means or '
        'influence to support the work of artists, writers, etc. This includes those who commission and pay for '
        'individual works.',
 'pbd': 'A person or organization who presides over the elaboration of a collective work to ensure its coherence or '
        'continuity. This includes editors-in-chief, literary editors, editors of series, etc.',
 'pbl': 'A person or organization responsible for publishing, releasing, or issuing a resource.',
 'pdr': 'A person or organization with primary responsibility for all essential aspects of a project, has overall '
        'responsibility for managing projects, or provides overall direction to a project manager.',
 'pfr': 'A person who corrects printed matter. For manuscripts, use corrector [crr].',
 'pht': 'A person, family, or organization responsible for creating a photographic work.',
 'plt': 'A person, family, or organization involved in manufacturing a manifestation by preparing plates used in the '
        'production of printed images and/or text.',
 'pma': 'An organization (usually a government agency) that issues permits under which work is accomplished.',
 'pmn': 'A person responsible for all technical and business matters in a production.',
 'pnc': 'A person or organization responsible for producing the initial line drawings based on a script in the '
        'creation of comic books, graphic novels, animation, etc.',
 'pop': 'A person or organization who prints illustrations from plates.',
 'ppm': 'A person or organization responsible for the production of paper, usually from wood, cloth, or other fibrous '
        'material.',
 'ppt': 'A performer contributing to a resource by manipulating, controlling, or directing puppets or marionettes in a '
        'moving image production or a musical or dramatic presentation or entertainment.',
 'pra': 'A person who is the faculty moderator of an academic disputation, normally proposing a thesis and '
        'participating in the ensuing disputation.',
 'prc': 'A person or organization primarily responsible for performing or initiating a process, such as is done with '
        'the collection of metadata sets.',
 'prd': 'A person or organization associated with the production (props, lighting, special effects, etc.) of a musical '
        'or dramatic presentation or entertainment.',
 'pre': 'A person or organization mentioned in an “X presents” credit for moving image materials and who is associated '
        'with production, finance, or distribution in some way. A vanity credit; in early years, normally the head of '
        'a studio.',
 'prf': 'A person contributing to a resource by performing music, acting, dancing, speaking, etc., often in a musical '
        'or dramatic presentation, etc. If specific codes are used, [prf] is used for a person whose principal skill '
        'is not known or specified.',
 'prg': 'A person, family, or organization responsible for creating a computer program.',
 'prm': 'A person or organization who makes a relief, intaglio, or planographic printing surface.',
 'prn': 'An organization that is responsible for financial, technical, and organizational management of a production '
        'for stage, screen, audio recording, television, webcast, etc.',
 'pro': 'A person, family, or organization responsible for most of the business aspects of a production for screen, '
        'audio recording, television, webcast, etc. The producer is generally responsible for fund raising, managing '
        'the production, hiring key personnel, arranging for distributors, etc.',
 'prp': 'The place of production (e.g., inscription, fabrication, construction, etc.) of a resource in an unpublished '
        'form.',
 'prs': 'A person or organization responsible for designing the overall visual appearance of a moving image '
        'production.',
 'prt': 'A person, family, or organization involved in manufacturing a manifestation of printed text, notated music, '
        'etc., from type or plates, such as a book, newspaper, magazine, broadside, score, etc.',
 'prv': 'A person or organization who produces, publishes, manufactures, or distributes a resource if specific codes '
        'are not desired (e.g. [mfr], [pbl]).',
 'pta': 'A person or organization that applied for a patent.',
 'pte': 'A plaintiff against whom an appeal is taken from one court or jurisdiction to another to reverse the '
        'judgment, usually in a legal proceeding.',
 'ptf': 'A person or organization who brings a suit in a civil proceeding.',
 'pth': 'A person or organization that was granted the patent referred to by the item.',
 'ptt': 'A plaintiff who takes an appeal from one court or jurisdiction to another to reverse the judgment, usually in '
        'a legal proceeding.',
 'pup': 'The place where a resource is published.',
 'rap': 'A person responsible for reporting on the proceedings of meetings of an organization.',
 'rbr': 'A person or organization responsible for parts of a work, often headings or opening parts of a manuscript, '
        'that appear in a distinctive color, usually red.',
 'rcd': 'A person or organization who uses a recording device to capture sounds and/or video during a recording '
        'session, including field recordings of natural sounds, folkloric events, music, etc.',
 'rce': 'A person contributing to a resource by supervising the technical aspects of a sound or video recording '
        'session.',
 'rcp': 'A person, family, or organization to whom the correspondence in a work is addressed.',
 'rdd': 'A director responsible for the general management and supervision of a radio program.',
 'red': 'A person or organization who writes or develops the framework for an item without being intellectually '
        'responsible for its content.',
 'ren': 'A person or organization who prepares drawings of architectural designs (i.e., renderings) in accurate, '
        'representational perspective to show what the project will look like when completed.',
 'res': 'A person or organization responsible for performing research.',
 'rev': 'A person or organization responsible for the review of a book, motion picture, performance, etc.',
 'rpc': 'A producer responsible for most of the business aspects of a radio program.',
 'rps': 'An organization that hosts data or material culture objects and provides services to promote long term, '
        'consistent and shared use of those data or objects.',
 'rpt': 'A person or organization who writes or presents reports of news or current events on air or in print.',
 'rpy': 'A person or organization legally responsible for the content of the published material.',
 'rse': 'A respondent against whom an appeal is taken from one court or jurisdiction to another to reverse the '
        'judgment, usually in an equity proceeding.',
 'rsg': 'A person or organization, other than the original choreographer or director, responsible for restaging a '
        'choreographic or dramatic work and who contributes minimal new content.',
 'rsp': 'A person or organization who makes an answer to the courts pursuant to an application for redress (usually in '
        'an equity proceeding) or a candidate for a degree who defends or opposes a thesis provided by the praeses in '
        'an academic disputation.',
 'rsr': 'A person, family, or organization responsible for the set of technical, editorial, and intellectual '
        'procedures aimed at compensating for the degradation of an item by bringing it back to a state as close as '
        'possible to its original condition.',
 'rst': 'A respondent who takes an appeal from one court or jurisdiction to another to reverse the judgment, usually '
        'in an equity proceeding.',
 'rth': 'A person who directed or managed a research project.',
 'rtm': 'A person who participated in a research project but whose role did not involve direction or management of it.',
 'rxa': 'A person that manipulates, recombines, mixes and reproduces previously-recorded sounds.',
 'sad': 'A person or organization who brings scientific, pedagogical, or historical competence to the conception and '
        'realization on a work, particularly in the case of audio-visual items.',
 'sce': 'A person or organization who is the author of a motion picture screenplay, generally the person who wrote the '
        'scenarios for a motion picture during the silent era.',
 'scl': 'An artist responsible for creating a three-dimensional work by modeling, carving, or similar technique.',
 'scr': 'A person who is an amanuensis and for a writer of manuscripts proper. For a person who makes pen-facsimiles, '
        'use facsimilist [fac].',
 'sde': 'A person responsible for recording sound on set during filmmaking or television production for inclusion in '
        'the finished product, or for use by the sound designer, sound effects editors, or foley artists.',
 'sds': 'A person who produces and reproduces the sound score (both live and recorded), the installation of '
        'microphones, the setting of sound levels, and the coordination of sources of sound for a production.',
 'sec': 'A person or organization who is a recorder, redactor, or other person responsible for expressing the views of '
        'a organization.',
 'sfx': 'A person or organization responsible for the activities of workers engaged in designing and creating on-set '
        'special effects appearing in a moving image or sound recording, such as on-set mechanical effects and '
        'in-camera option effects.',
 'sgd': 'A person or organization contributing to a stage resource through the overall management and supervision of a '
        'performance.',
 'sgn': 'A person whose signature appears without a presentation or other statement indicative of provenance. When '
        'there is a presentation statement, use inscriber [ins].',
 'sht': 'A person or organization that supports (by allocating facilities, staff, or other resources) a project, '
        'program, meeting, event, data objects, material culture objects, or other entities capable of support.',
 'sll': 'A former owner of an item who sold that item to another owner.',
 'sng': 'A performer contributing to a resource by using his/her/their voice, with or without instrumental '
        "accompaniment, to produce music. A singer's performance may or may not include actual words.",
 'spk': 'A performer contributing to a resource by speaking words, such as a lecture, speech, etc.',
 'spn': 'A person, family, or organization sponsoring some aspect of a resource, e.g., funding research, sponsoring an '
        'event.',
 'spy': 'A person or organization who is identified as the party of the second part. In the case of transfer of '
        'rights, this is the assignee, transferee, licensee, grantee, etc. Multiple parties can be named jointly as '
        'the second party.',
 'srv': 'A person, family, or organization contributing to a cartographic resource by providing measurements or '
        'dimensional relationships for the geographic area represented.',
 'std': 'A person who translates the rough sketches of the art director into actual architectural structures for a '
        'theatrical presentation, entertainment, motion picture, etc. Set designers draw the detailed guides and '
        'specifications for building the set.',
 'stg': 'An entity in which the activity or plot of a work takes place, e.g. a geographic place, a time period, a '
        'building, an event.',
 'stl': "A performer contributing to a resource by relaying a creator's original story with dramatic or theatrical "
        'interpretation.',
 'stm': 'A person who is in charge of everything that occurs on a performance stage, and who acts as chief of all '
        'crews and assistant to a director during rehearsals.',
 'stn': 'An organization responsible for the development or enforcement of a standard.',
 'str': 'A person or organization who creates a new plate for printing by molding or copying another printing surface.',
 'swd': 'A person who researches, designs, implements or tests software.',
 'tad': 'A person with expertise in a particular field who advises on the convincing portrayal of a subject to add '
        'authenticity to a work.',
 'tau': 'A person contributing to a television resource by writing or collaborating with a group of writers on a '
        'script for a television program, such as an episode of a television series.',
 'tcd': 'A person who is ultimately in charge of scenery, props, lights and sound for a production.',
 'tch': 'A performer contributing to a resource by giving instruction or providing a demonstration.',
 'ths': 'A person under whose supervision a degree candidate develops and presents a thesis, mémoire, or text of a '
        'dissertation.',
 'tld': 'A director responsible for the general management and supervision of a television program.',
 'tlg': 'A person invited to appear in a television program, often a television talk or variety show. Guests normally '
        'appear as themselves or as characters in skits.',
 'tlh': 'A person contributing to a television resource by leading a program that includes other guest, performers, '
        'etc.',
 'tlp': 'A producer responsible for most of the business aspects of a television program.',
 'trc': 'A person, family, or organization contributing to a resource by changing it from one system of notation to '
        'another. For a work transcribed for a different instrument or performing group, see Arranger [arr]. For '
        'makers of pen-facsimiles, use facsimilist [fac].',
 'trl': 'A person or organization who renders a text from one language into another, or from an older form of a '
        'language into the modern form.',
 'tyd': 'A person or organization who designs the type face used in a particular item.',
 'tyg': 'A person or organization primarily responsible for choice and arrangement of type used in an item. If the '
        'typographer is also responsible for other aspects of the graphic design of a book (e.g., Book designer '
        '[bkd]), codes for both functions may be needed.',
 'uvp': 'A place where a university that is associated with a resource is located, for example, a university where an '
        'academic dissertation or thesis was presented.',
 'vac': 'An actor contributing to a resource by providing the voice for characters in radio and audio productions and '
        'for animated characters in moving image works, as well as by providing voice overs in radio and television '
        'commercials, dubbed resources, etc.',
 'vdg': 'A person in charge of a video production, e.g. the video recording of a stage production as opposed to a '
        'commercial motion picture. The videographer may be the camera operator or may supervise one or more camera '
        'operators. Do not confuse with cinematographer.',
 'vfx': 'A person or organization responsible for the activities of workers engaged in designing and creating '
        'post-production visual effects appearing in a moving image.',
 'wac': 'A person, family, or organization contributing to an expression of a work by providing an interpretation or '
        'critical explanation of the original work.',
 'wal': 'A writer of words added to an expression of a musical work. For lyric writing in collaboration with a '
        'composer to form an original work, see lyricist.',
 'wam': 'A person or organization who writes significant material which accompanies a sound recording or other '
        'audiovisual material.',
 'wat': 'A person, family, or organization contributing to a non-textual resource by providing text for the '
        'non-textual work (e.g., writing captions for photographs, descriptions of maps).',
 'waw': 'A person, family, or organization contributing to a resource by providing an afterword to the original work.',
 'wdc': 'A person or organization who makes prints by cutting the image in relief on the plank side of a wood block.',
 'wde': 'A person or organization who makes prints by cutting the image in relief on the end-grain of a wood block.',
 'wfs': 'A person contributing to a motion picture resource by writing an original story expressly for the resource, '
        'not based on any other existing work. For the author of a screenplay, use screenwriter. For a person who '
        'adapts novels or stories for the screen, use adaptor.',
 'wft': 'A person contributing to a motion picture resource by writing dialogue or expository intertitles inserted '
        'intermittently between sequences of the film. Generally, the person who wrote the intertitles for a motion '
        'picture during the silent era.',
 'wfw': 'A person, family, or organization contributing to a resource by providing a foreword to the original work.',
 'win': 'A person, family, or organization contributing to a resource by providing an introduction to the original '
        'work.',
 'wit': 'Use for a person who verifies the truthfulness of an event or action.',
 'wpr': 'A person, family, or organization contributing to a resource by providing a preface to the original work.',
 'wst': 'A person, family, or organization contributing to a resource by providing supplementary textual content '
        '(e.g., an introduction, a preface) to the original work.',
 'wts': 'A person contributing to a television resource by writing an original story expressly for the resource, not '
        'based on any other existing work. For a person who writes a script for a television program, use television '
        'writer. For a person who adapts novels or stories for television, use adaptor.'}

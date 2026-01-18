import datetime
from copy import deepcopy


from LiuXin_tests.test_databases.test_db_properties.common_db_properties import (
    CommonDBProperties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_0_properties import (
    TestDB0Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_1_properties import (
    TestDB1Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_2_properties import (
    TestDB2Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_3_properties import (
    TestDB3Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_4_properties import (
    TestDB4Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_5_properties import (
    TestDB5Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_6_properties import (
    TestDB6Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_7_properties import (
    TestDB7Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_8_properties import (
    TestDB8Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_9_properties import (
    TestDB9Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_10_properties import (
    TestDB10Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_11_properties import (
    TestDB11Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_12_properties import (
    TestDB12Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_13_properties import (
    TestDB13Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_14_properties import (
    TestDB14Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_15_properties import (
    TestDB15Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_16_properties import (
    TestDB16Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_17_properties import (
    TestDB17Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_18_properties import (
    TestDB18Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_19_properties import (
    TestDB19Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_20_properties import (
    TestDB20Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_21_properties import (
    TestDB21Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_22_properties import (
    TestDB22Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_23_properties import (
    TestDB23Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_24_properties import (
    TestDB24Properties,
)
from LiuXin_tests.test_databases.test_db_properties.test_db_25_properties import (
    TestDB25Properties,
)

__all__ = ["TestDB0Properties",
           "TestDB1Properties", "TestDB2Properties",
           ]

# Stores the properties dof the given test database somewhere central where they can be easily updated
# Should be updated manually - the build should be reproducible.
# DO NOT DUMMY IT UP AND READ THE PROPERTIES OUT OF THE CREATED DATABASES
# One of the points of this is to validate that the test database are building correctly and reproducibly.


# Todo: Work the tests for test db 17 - to check all these values

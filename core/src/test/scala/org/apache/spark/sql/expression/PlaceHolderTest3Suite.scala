/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseTiSparkSuite

class PlaceHolderTest3Suite extends BaseTiSparkSuite {
  private val allCases = Seq[String](
    "select  count(1)  from full_data_type_table  where tp_double <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_double <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_double <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_double <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_double <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_double <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_double <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_double <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_double <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_double <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_double <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_double <> 32767",
    "select  count(1)  from full_data_type_table  where tp_double <> -32768",
    "select  count(1)  from full_data_type_table  where tp_double <> 127",
    "select  count(1)  from full_data_type_table  where tp_double <> -128",
    "select  count(1)  from full_data_type_table  where tp_double <> 0",
    "select  count(1)  from full_data_type_table  where tp_double <> 2017",
    "select  count(1)  from full_data_type_table  where tp_double <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> null",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 32767",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> -32768",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 127",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> -128",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 0",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 2017",
    "select  count(1)  from full_data_type_table  where tp_nvarchar <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_datetime <> null",
    "select  count(1)  from full_data_type_table  where tp_datetime <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_datetime <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_datetime <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_datetime <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_datetime <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_datetime <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> null",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_smallint <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_smallint <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 32767",
    "select  count(1)  from full_data_type_table  where tp_smallint <> -32768",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 127",
    "select  count(1)  from full_data_type_table  where tp_smallint <> -128",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 0",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 2017",
    "select  count(1)  from full_data_type_table  where tp_smallint <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_date <> null",
    "select  count(1)  from full_data_type_table  where tp_date <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_date <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_date <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_date <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_date <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_date <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> null",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_varchar <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_varchar <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 32767",
    "select  count(1)  from full_data_type_table  where tp_varchar <> -32768",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 127",
    "select  count(1)  from full_data_type_table  where tp_varchar <> -128",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 0",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 2017",
    "select  count(1)  from full_data_type_table  where tp_varchar <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> null",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 32767",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> -32768",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 127",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> -128",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 0",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 2017",
    "select  count(1)  from full_data_type_table  where tp_mediumint <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_longtext <> null",
    "select  count(1)  from full_data_type_table  where tp_int <> null",
    "select  count(1)  from full_data_type_table  where tp_int <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_int <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_int <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_int <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_int <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_int <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_int <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_int <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_int <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_int <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_int <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_int <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_int <> 32767",
    "select  count(1)  from full_data_type_table  where tp_int <> -32768",
    "select  count(1)  from full_data_type_table  where tp_int <> 127",
    "select  count(1)  from full_data_type_table  where tp_int <> -128",
    "select  count(1)  from full_data_type_table  where tp_int <> 0",
    "select  count(1)  from full_data_type_table  where tp_int <> 2017",
    "select  count(1)  from full_data_type_table  where tp_int <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_tinytext <> null",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> null",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> 'PingCAP'",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_timestamp <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_mediumtext <> null",
    "select  count(1)  from full_data_type_table  where tp_real <> null",
    "select  count(1)  from full_data_type_table  where tp_real <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_real <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_real <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_real <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_real <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_real <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_real <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_real <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_real <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_real <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_real <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_real <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_real <> 32767",
    "select  count(1)  from full_data_type_table  where tp_real <> -32768",
    "select  count(1)  from full_data_type_table  where tp_real <> 127",
    "select  count(1)  from full_data_type_table  where tp_real <> -128",
    "select  count(1)  from full_data_type_table  where tp_real <> 0",
    "select  count(1)  from full_data_type_table  where tp_real <> 2017",
    "select  count(1)  from full_data_type_table  where tp_real <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_text <> null",
    "select  count(1)  from full_data_type_table  where tp_decimal <> null",
    "select  count(1)  from full_data_type_table  where tp_decimal <> '2017-11-02'",
    "select  count(1)  from full_data_type_table  where tp_decimal <> '2017-10-30'",
    "select  count(1)  from full_data_type_table  where tp_decimal <> '2017-09-07 11:11:11'",
    "select  count(1)  from full_data_type_table  where tp_decimal <> '2017-11-02 08:47:43'",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 'fYfSp'",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 9223372036854775807",
    "select  count(1)  from full_data_type_table  where tp_decimal <> -9223372036854775808",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 1.7976931348623157E308",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 3.14159265358979",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 2.34E10",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 2147483647",
    "select  count(1)  from full_data_type_table  where tp_decimal <> -2147483648",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 32767",
    "select  count(1)  from full_data_type_table  where tp_decimal <> -32768",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 127",
    "select  count(1)  from full_data_type_table  where tp_decimal <> -128",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 0",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 2017",
    "select  count(1)  from full_data_type_table  where tp_decimal <> 2147868.65536",
    "select  count(1)  from full_data_type_table  where tp_binary <> null"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query, query.replace("full_data_type_table", "full_data_type_table_j"))
      }
    }
  }

}

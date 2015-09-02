/*
 * Copyright 2004-2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lucene.store.jdbc.dialect;

import java.util.Map;

import com.github.lucene.store.jdbc.JdbcDirectorySettings;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;
import com.github.lucene.store.jdbc.index.FileJdbcIndexOutput;
import com.github.lucene.store.jdbc.index.RAMAndFileJdbcIndexOutput;
import com.github.lucene.store.jdbc.index.RAMJdbcIndexOutput;
import com.github.lucene.store.jdbc.index.oracle.OracleFileJdbcIndexOutput;
import com.github.lucene.store.jdbc.index.oracle.OracleRAMAndFileJdbcIndexOutput;
import com.github.lucene.store.jdbc.index.oracle.OracleRAMJdbcIndexOutput;

/**
 * An Oralce 9i dialect, changes all to work with Oracle related index output.
 *
 * @author kimchy
 */
public class Oracle9Dialect extends OracleDialect {

    @Override
    public void processSettings(final JdbcDirectorySettings settings) {
        final Map<String, JdbcFileEntrySettings> filesEntrySettings = settings.getFileEntrySettings();
        for (final JdbcFileEntrySettings jdbcFileEntrySettings : filesEntrySettings.values()) {
            final JdbcFileEntrySettings fileEntrySettings = jdbcFileEntrySettings;
            try {
                Class<?> indexOutputClass = fileEntrySettings.getSettingAsClass(
                        JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMAndFileJdbcIndexOutput.class);
                if (indexOutputClass.equals(RAMAndFileJdbcIndexOutput.class)) {
                    indexOutputClass = OracleRAMAndFileJdbcIndexOutput.class;
                } else if (indexOutputClass.equals(RAMJdbcIndexOutput.class)) {
                    indexOutputClass = OracleRAMJdbcIndexOutput.class;
                } else if (indexOutputClass.equals(FileJdbcIndexOutput.class)) {
                    indexOutputClass = OracleFileJdbcIndexOutput.class;
                }
                fileEntrySettings.setClassSetting(JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, indexOutputClass);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Failed to find class", e);
            }
        }
    }
}

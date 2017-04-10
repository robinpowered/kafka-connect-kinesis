/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.kinesis;

import com.github.jcustenborder.kafka.connect.utils.BaseDocumentationTest;
import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentationTest extends BaseDocumentationTest {
  private static final Logger log = LoggerFactory.getLogger(DocumentationTest.class);

  @Override
  protected String[] packages() {
    return new String[]{this.getClass().getPackage().getName()};
  }

  @Test
  public void schema() {
    Schema[] schemas = new Schema[]{
        RecordConverter.SCHEMA_KINESIS_KEY,
        RecordConverter.SCHEMA_KINESIS_VALUE
    };

    StringBuilder builder = new StringBuilder();
    for (Schema schema : schemas) {
      builder.append(MarkdownFormatter.toMarkdown(schema));
      builder.append('\n');
    }

    log.info("\n{}", builder);
  }
}

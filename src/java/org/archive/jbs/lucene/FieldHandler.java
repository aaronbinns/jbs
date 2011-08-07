/*
 * Copyright 2010 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.archive.jbs.lucene;

import org.apache.lucene.document.*;
import org.archive.jbs.Document;

/**
 * Simple interface for family of implementations which "handle" a
 * field by taking information related to that field from the
 * Document, creating a Lucene Field object and adding it to
 * the Document.
 */
public interface FieldHandler
{
  public void handle( org.apache.lucene.document.Document luceneDocument, Document document );
}

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

package org.archive.hadoop;

/**
 * Simple interface for any number of implementors to provide Map-like
 * functionality returning a value for a key.
 *
 * By convention, for any key not found, an empty string ("") is
 * returned.
 *
 * Also, all strings returned are assumed to be trim()'d before being
 * returned.
 */
public interface Document
{
  public String get( String key );
}

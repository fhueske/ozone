/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.io.pax.io.compression;

/**
 * Common interface for compression algorithms used
 * to compress the columns of the row groups.
 *
 */
public interface ICompressor {

    /**
     * Compresses the data.
     * <p/>
     * Continuous calls to this method must produce
     * separately compressed blocks of byte data.
     *
     * @param data the data to be compressed.
     * @return the compressed data
     */
    byte[] compress(byte[] data);

    /**
     * Exceptions thrown by {@link ICompressor} instances.
     */
    static class CompressorException extends RuntimeException {

        public CompressorException(String message) {
            super(message);
        }

        public CompressorException(String message, Throwable cause) {
            super(message, cause);
        }

        public CompressorException(Throwable cause) {
            super(cause);
        }
    }
}

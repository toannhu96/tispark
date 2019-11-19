package com.pingcap.tikv.columnar;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A helper class to create {@link ColumnarBatch} from {@link TiChunk}
 */
public final class TiColumnarBatchHelper {
  public static ColumnarBatch createColumnarBatch(TiChunk chunk) {
    TiColumnVectorAdapter[] columns = new TiColumnVectorAdapter[chunk.numOfCols()];
    for(int i = 0; i < chunk.numOfCols(); i++) {
      columns[i] = new TiColumnVectorAdapter(chunk.column(i));
    }
    ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(chunk.numOfRows());
    return batch;
  }
}

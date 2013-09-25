package logprocessor.reporters;

import logprocessor.enums.Columns;
import logprocessor.enums.ReportNames;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.text.DateFormatter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class TimeBucketedReporterAssembly extends SubAssembly {
  private static final long serialVersionUID = -6172682286693758285L;

  public TimeBucketedReporterAssembly(String name, int secondsDelta) {
    long msDelta = secondsDelta * 1000;

    Pipe head = new Pipe(name);
    head = new Each(head, new ExpressionFunction(new Fields(Columns.ModifiedTimeStamp), "ts - (ts%" + msDelta + ")", long.class), Fields.ALL);
    DateFormatter formatter = new DateFormatter(new Fields(Columns.BucketedDateTime), "yyyy-MM-dd HH:mm:ss" );
    head = new Each(head, new Fields(Columns.ModifiedTimeStamp), formatter, Fields.ALL);

    // Grouped by time
    Pipe timeGrouped = new GroupBy(head, new Fields(Columns.BucketedDateTime));
    timeGrouped = new Every(timeGrouped, new Count(new Fields(Columns.RequestCount)));
    timeGrouped = new Every(timeGrouped, new Fields(Columns.Bytes), new Sum(new Fields(Columns.TotalBytes), long.class));

    // Total request count (add additional GroupBy to force 1 reducer)
    Pipe totalRequestCount = new Each(timeGrouped, new Fields(Columns.BucketedDateTime, Columns.RequestCount), new Identity());
    totalRequestCount = new GroupBy(totalRequestCount, new Fields(Columns.BucketedDateTime, Columns.RequestCount));
    totalRequestCount = new Pipe(name + "-" + ReportNames.RequestCount, totalRequestCount);

    // Bytes transferred.
    Pipe bytesTransferred = new Each(timeGrouped, new Fields(Columns.BucketedDateTime,Columns.TotalBytes), new Identity());
    bytesTransferred = new GroupBy(bytesTransferred,new Fields(Columns.BucketedDateTime, Columns.TotalBytes));
    bytesTransferred = new Pipe(name+"-"+ReportNames.BytesTransferred, bytesTransferred);

    // Grouped by time and response code.
    Pipe timeResponseGrouped = new GroupBy(head, new Fields(Columns.BucketedDateTime, Columns.HTTPResponse));
    timeResponseGrouped = new Every(timeResponseGrouped, new Count(new Fields(Columns.RequestCount)));
    // This is just to optimize and have more reducers
    timeResponseGrouped = new GroupBy(timeResponseGrouped, new Fields(Columns.BucketedDateTime, Columns.HTTPResponse, Columns.RequestCount));
    timeResponseGrouped = new Pipe(name + "-" + ReportNames.RequestCountByHttpResponse, timeResponseGrouped);

    setTails(totalRequestCount, timeResponseGrouped, bytesTransferred);
  }
}

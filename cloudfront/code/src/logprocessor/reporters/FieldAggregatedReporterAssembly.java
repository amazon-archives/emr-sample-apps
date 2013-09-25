package logprocessor.reporters;

import logprocessor.enums.Columns;
import logprocessor.enums.ReportNames;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class FieldAggregatedReporterAssembly extends SubAssembly {
  private static final long serialVersionUID = -8166392284171660686L;

  public FieldAggregatedReporterAssembly(String name, String fn) {
    Pipe head = new Pipe(name);
    Pipe groupByField = new GroupBy(head, new Fields(fn));
    groupByField = new Every(groupByField, new Count(new Fields(Columns.RequestCount)));
    groupByField = new Every(groupByField, new Fields(Columns.Bytes), new Sum(new Fields(Columns.TotalBytes), long.class));

    // Total request count
    Pipe totalRequestCount = new GroupBy(groupByField,
        new Fields(Columns.RequestCount, fn),
        true);
    totalRequestCount = new Each(totalRequestCount, new Fields(fn, Columns.RequestCount), new Identity());
    totalRequestCount = new Pipe(name + "-" + ReportNames.RequestCount, totalRequestCount);

    // Bytes transferred
    Pipe bytesTransferred = new GroupBy(groupByField,
        new Fields(Columns.TotalBytes, fn),
        true);
    bytesTransferred = new Each(bytesTransferred, new Fields(fn,Columns.TotalBytes), new Identity());
    bytesTransferred = new Pipe(name + "-" + ReportNames.BytesTransferred, bytesTransferred);

    // Group by field and response
    Pipe groupByFieldAndResponse = new GroupBy(head, new Fields(fn, Columns.HTTPResponse));
    groupByFieldAndResponse = new Every(groupByFieldAndResponse, new Count(new Fields(Columns.RequestCount)));
    groupByFieldAndResponse = new GroupBy(name + "-" + ReportNames.RequestCountByHttpResponse,
        groupByFieldAndResponse,
        new Fields(Columns.RequestCount, Columns.HTTPResponse, fn),
        true);

    setTails(totalRequestCount, groupByFieldAndResponse, bytesTransferred);
  }
}

package logprocessor.s3copy;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

public class DatePathFilter {
  private static final long serialVersionUID = -4588733198730825887L;
  
  private Date start;
  private Date end;
  private final Pattern datePattern;
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

  public DatePathFilter(final String startDate, final String endDate) throws ParseException  {
    // Compile the pattern
    String pattern = ".*\\.(\\d+-\\d+-\\d+-\\d+)\\..*";
    datePattern = Pattern.compile(pattern);
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    this.start = dateFormat.parse(startDate);
    this.end = dateFormat.parse(endDate);
  }
  
  public boolean accept(Path path) {
    Matcher m = datePattern.matcher(path.toString());
    
    if (!m.matches()) {
      return false;
    }
    
    // Get the last match
    String dateTime = m.group(m.groupCount()); // dateTime format yyyy-MM-dd-HH
    Date recordDate;
    try {
      recordDate = dateFormat.parse(dateTime);
    } catch (ParseException ex) {
      throw new RuntimeException("Error parsing date:" + dateTime, ex);
    }
    
    return recordDate.equals(start)
        || recordDate.equals(end)
        || (recordDate.after(start) && recordDate.before(end));
  }
}

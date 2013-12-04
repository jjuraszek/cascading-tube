package jj.tube.tap;

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.*;
import cascading.util.CloseableIterator;
import cascading.util.SingleCloseableInputIterator;
import cascading.util.SingleValueCloseableIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@SuppressWarnings("unchecked")
public class MemoryTap extends Tap {
  private final String id = "ID_" + System.currentTimeMillis();
  public final Set<String> content = Sets.newHashSet();

  private CloseableIterator input;

  private MemoryTap(){

  }

  public static MemoryTap input(final String[] fields, Set<Object[]> input) {
    MemoryTap tap = new MemoryTap();
    tap.setScheme(new NullScheme(new Fields(fields), Fields.UNKNOWN) {
      @Override
      public boolean source(FlowProcess flowProcess, SourceCall sourceCall) throws IOException {
        Iterator<Tuple> tuples = (Iterator<Tuple>) sourceCall.getInput();
        boolean newDate = tuples.hasNext();
        if(newDate){
          sourceCall.getIncomingEntry().setTuple(tuples.next());
        }
        return newDate;
      }
    });
    List<Tuple> tuples = Lists.newLinkedList();
    for (Object[] tuple : input) {
      tuples.add(new Tuple(tuple));
    }

    final Iterator it = tuples.iterator();
    tap.input = new SingleValueCloseableIterator(it){
      @Override
      public void close() throws IOException {}
    };
    return tap;
  }

  public static MemoryTap output() {
    MemoryTap tap = new MemoryTap();
    tap.setScheme(new NullScheme() {
      @Override
      public void sink(FlowProcess flowProcess, SinkCall sinkCall) throws IOException {
        TupleEntry tuple = sinkCall.getOutgoingEntry();
        ((Set<String>)sinkCall.getOutput()).add(tuple.getTuple().toString(","));
      }
    });
    return tap;
  }


  @Override
  public String getIdentifier() {
    return id;
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess flowProcess, Object o) throws IOException {
    return new TupleEntrySchemeIterator(flowProcess, getScheme(), input, getIdentifier());
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess flowProcess, Object o) throws IOException {
    return new TupleEntrySchemeCollector(flowProcess, getScheme(), content, getIdentifier());
  }

  @Override
  public boolean createResource(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteResource(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean resourceExists(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getModifiedTime(Object conf) throws IOException {
    throw new UnsupportedOperationException();
  }
}

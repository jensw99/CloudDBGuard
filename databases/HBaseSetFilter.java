package databases;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import misc.FilterProtos;

public class HBaseSetFilter extends FilterBase {
	private byte[] columnFamily;
	private byte[] value;
	private boolean include;
	
	public HBaseSetFilter(byte[] columnFamily, byte[] value) {
		this.value = value;
		this.columnFamily = columnFamily;
		this.include = false;
	}
	@Override
	public boolean filterRow() {
		return !include;
	}
	
	@Override
	public void reset() {
		include = false;
	}
	
	
	@Override
	public boolean hasFilterRow() {
	   return true;
	}
	
	@Override
	public byte[] toByteArray() {
	    final FilterProtos.HBaseSetFilter.Builder builder = FilterProtos.HBaseSetFilter.newBuilder();
	    builder.setColumnFamily(ByteString.copyFrom(columnFamily));
	    builder.setValue(ByteString.copyFrom(value));

	    return builder.build().toByteArray();
	}
	
	public static HBaseSetFilter parseFrom(final byte[] rawBytes)
	        throws DeserializationException {

	    try {
	        FilterProtos.HBaseSetFilter proto;
	        proto = FilterProtos.HBaseSetFilter.parseFrom(rawBytes);

	        return new HBaseSetFilter(proto.getColumnFamily().toByteArray(), proto.getValue().toByteArray());
	    } catch (InvalidProtocolBufferException ex) {
	        throw new DeserializationException(ex);
	    }
	}
	
	@Override
	public ReturnCode filterCell(final Cell c) {
		if(CellUtil.matchingFamily(c, columnFamily)) {
			if(CellUtil.matchingValue(c, value)) {
				this.include = true;
				return ReturnCode.INCLUDE_AND_NEXT_COL;
			}
		}
		return ReturnCode.INCLUDE;
	}

}

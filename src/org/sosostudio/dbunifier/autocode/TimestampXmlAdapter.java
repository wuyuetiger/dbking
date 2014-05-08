package org.sosostudio.dbunifier.autocode;

import java.sql.Timestamp;
import java.util.Calendar;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class TimestampXmlAdapter extends XmlAdapter<Calendar, Timestamp> {

	public Calendar marshal(Timestamp t) throws Exception {
		Calendar c = Calendar.getInstance();
		c.setTime(t);
		return c;
	}

	public Timestamp unmarshal(Calendar c) throws Exception {
		return new Timestamp(c.getTime().getTime());
	}

}

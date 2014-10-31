package edu.rosehulman.postcn;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class TimestampInterceptor implements Interceptor {

	public void close() {
		//do nothing
	}

	public void initialize() {
		//do nothing
	}

	public Event intercept(Event event) {
		
		StringBuilder builder = new StringBuilder();
		builder.append(new String(event.getBody()));
		builder.append("\tParse Time: ");
		builder.append(new Timestamp((new Date()).getTime()));
		event.setBody(builder.toString().getBytes());
		
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		List<Event> eventList = new ArrayList<Event>();
		  for (Event event : events) {
	      Event outEvent = intercept(event);
	      if(outEvent !=null){
	    	  eventList.add(outEvent);
	      }
	    }
	    return eventList;
	}
	
	public static class Builder implements Interceptor.Builder {

		public void configure(Context arg0) {
			//leave blank;
		}

		public Interceptor build() {
			return new TimestampInterceptor();
		}
		
	}

}

package edu.rosehulman.postcn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class TextInterceptor implements Interceptor {

	public void close() {
		//do nothing
	}

	public void initialize() {
		//do nothing.
	}

	public Event intercept(Event event) {
		String hostname;
		Map<String, String> headers = event.getHeaders();
		if ((hostname = headers.get("host")) != null) {
			hostname = hostname.split(".")[0];//separate off the first element.
		}
		else if (System.getProperty("os.name").startsWith("Windows")) {
		    hostname = System.getenv("COMPUTERNAME");
		} else {
		    hostname = System.getenv("HOSTNAME");
		}
		headers.put("host", hostname);
		event.setHeaders(headers);
		
		//at this point we have set the host to be only the name of the computer.
		
		//filter out events if they contain "No Command....
		
		if (event.getBody().toString().contains("No commands sent from the Server")) {
			return null;
		}
		else {
			return event;
		}
	}

	//Interceptor could be an abstract class so people didn't always have to implement this function
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

}

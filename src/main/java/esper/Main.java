package esper;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;

public class Main {
	static {
		SLF4JBridgeHandler.install();
	}

	public static void main(String[] args) {
		Logger log = LoggerFactory.getLogger(Main.class);
		Configuration esperClientConfiguration = new Configuration();

		// Setup Esper and define a message Type "TwitterEvent"
		EPServiceProvider epServiceProvider = EPServiceProviderManager.getDefaultProvider(esperClientConfiguration);
		{
			Map<String, Object> eventDef = new HashMap<>();
			eventDef.put("id", int.class);
			eventDef.put("messageText", String.class);
			eventDef.put("likeCount", long.class);
			eventDef.put("userId", String.class);
			eventDef.put("sentiment", String.class);
			eventDef.put("keyword", String.class);

			epServiceProvider.getEPAdministrator().getConfiguration().addEventType("TwitterEvent", eventDef);
		}

		// Create a query and add a listener to it
		{
			String expression = "select keyword, sentiment, count(sentiment) as counter "
					+ "from TwitterEvent.win:time(10) group by sentiment, keyword";

			EPStatement epStatement = epServiceProvider.getEPAdministrator().createEPL(expression);

			Map<String, Long> latestSentimentToCountMap = new HashMap<>();

			epStatement.addListener((EventBean[] newEvents, EventBean[] oldEvents) -> {
				if (newEvents == null || newEvents.length < 1) {
					log.warn("Received null event or length < 1: " + newEvents);
					return;
				}

				EventBean event = newEvents[0];

				String key = (String) event.get("keyword") + "-" + (String) event.get("sentiment");
				latestSentimentToCountMap.put(key, (Long) event.get("counter"));

				log.info("--------------------------------------------------------------");
				for (Entry<String, Long> entry : latestSentimentToCountMap.entrySet())
					log.info("Twitter {} = {}", entry.getKey(), entry.getValue());

			});

			epStatement.start();
		}

		// Send some random data to Esper
		{
			String sentiments[] = { "POSITIVE", "NEUTRAL", "NEGATIVE" };
			String keywords[] = { "BMW", "Audi", "Porsche" };

			Random r = new Random();

			IntStream.range(1, 100).forEach(i -> {
				// Create Hash Map
				Map<String, Object> socialEvent = new HashMap<>();
				socialEvent.put("id", r.nextInt(10000));
				socialEvent.put("userId", r.nextInt(1000));
				socialEvent.put("messageText", "Bla " + r.nextInt());
				socialEvent.put("likeCount", 0);
				socialEvent.put("sentiment", sentiments[r.nextInt(sentiments.length)]);
				socialEvent.put("keyword", keywords[r.nextInt(keywords.length)]);

				epServiceProvider.getEPRuntime().sendEvent(socialEvent, "TwitterEvent");

				try {
					Thread.sleep(r.nextInt(1000));
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}

	}

}

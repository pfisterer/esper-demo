package esper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;

public class EsperFinance {

	static {
		Main.setupLogging();
	}

	public static void streamToEsper(EPRuntime esper, String file, String key) {
		InputStream instream = EsperFinance.class.getClassLoader().getResourceAsStream(file);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Random r = new Random();

		Iterable<CSVRecord> records;
		try {
			records = CSVFormat.DEFAULT.withHeader("Date", "Open", "High", "Low", "Close", "Volume", "Adj Close")
					.withSkipHeaderRecord()
					.parse(new InputStreamReader(instream));

			for (CSVRecord record : records) {
				Map<String, Object> event = new HashMap<>();
				event.put("key", key);
				event.put("closing", Double.parseDouble(record.get("Close")));
				event.put("date", format.parse(record.get("Date")));

				esper.sendEvent(event, "StockEvent");

				Thread.sleep(r.nextInt(200));
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void setupQuery(EPAdministrator esper, Logger log) {
		String expression = "select AVG(closing) as avg " + "from StockEvent.win:time(10 seconds)";

		EPStatement epStatement = esper.createEPL(expression);

		epStatement.addListener((EventBean[] newEvents, EventBean[] oldEvents) -> {
			if (newEvents == null || newEvents.length < 1) {
				log.warn("Received null event or length < 1: " + newEvents);
				return;
			}

			EventBean event = newEvents[0];

			log.info("" + event.get("avg"));

		});

		epStatement.start();
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Logger log = LoggerFactory.getLogger(Main.class);
		Configuration esperClientConfiguration = new Configuration();

		// Setup Esper and define a message Type "TwitterEvent"
		EPServiceProvider epServiceProvider = EPServiceProviderManager.getDefaultProvider(esperClientConfiguration);
		{
			Map<String, Object> eventDef = new HashMap<>();
			eventDef.put("key", String.class);
			eventDef.put("closing", Double.class);
			eventDef.put("date", java.util.Date.class);
			epServiceProvider.getEPAdministrator().getConfiguration().addEventType("StockEvent", eventDef);
		}

		EPRuntime esper = epServiceProvider.getEPRuntime();

		setupQuery(epServiceProvider.getEPAdministrator(), log);
		new Thread(() -> streamToEsper(esper, "yahoo-finance-cisco.csv", "cisco")).start();
		new Thread(() -> streamToEsper(esper, "yahoo-finance-apple.csv", "apple")).start();
		new Thread(() -> streamToEsper(esper, "yahoo-finance-ibm.csv", "ibm")).start();

	}
}

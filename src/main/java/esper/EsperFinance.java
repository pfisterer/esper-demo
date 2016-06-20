package esper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;

public class EsperFinance {

	static {
		Main.setupLogging();
	}

	static class StockEvent {
		String key;
		Double closing;
		Date date;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Double getClosing() {
			return closing;
		}

		public void setClosing(Double closing) {
			this.closing = closing;
		}

		public Date getDate() {
			return date;
		}

		public void setDate(Date date) {
			this.date = date;
		}

	}

	static class GrowthEvent {
		String key;

		Double growth;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Double getGrowth() {
			return growth;
		}

		public void setGrowth(Double growth) {
			this.growth = growth;
		}

	}

	public static void setupGrowthQuery(EPServiceProvider esper, Logger log) {
		esper.getEPAdministrator().getConfiguration().addEventType("StockEvent", StockEvent.class);

		String expression = "select a.key as key, b.closing - a.closing as growth from pattern[every a=StockEvent -> timer:interval(1 sec)  -> b=StockEvent(key=a.key)]";

		EPStatement epStatement = esper.getEPAdministrator().createEPL(expression);

		epStatement.addListener((EventBean[] newEvents, EventBean[] oldEvents) -> {
			if (newEvents == null || newEvents.length < 1) {
				log.warn("Received null event or length < 1: " + newEvents);
				return;
			}

			EventBean event = newEvents[0];
			Double growth = (Double) event.get("growth");
			String key = (String) event.get("key");

			log.info("Growth: " + growth + " (" + key + ")");

			GrowthEvent growthEvent = new GrowthEvent();
			growthEvent.key = key;
			growthEvent.growth = growth;
			esper.getEPRuntime().sendEvent(growthEvent);
		});

		epStatement.start();
	}

	public static void setupComparyStocksQuery(EPServiceProvider esper, Logger log) {

		esper.getEPAdministrator().getConfiguration().addEventType("GrowthMessage", GrowthEvent.class);

		String expression = "SELECT * From GrowthMessage(key='apple').win:length(1) as gm1, GrowthMessage(key='cisco').win:length(1) as gm2";

		EPStatement epStatement = esper.getEPAdministrator().createEPL(expression);

		epStatement.addListener((EventBean[] newEvents, EventBean[] oldEvents) -> {
			if (newEvents == null || newEvents.length < 1) {
				log.warn("Received null event or length < 1: " + newEvents);
				return;
			}

			EventBean event = newEvents[0];
			Double gm2Growth = (Double) event.get("gm2.growth");
			Double gm1Growth = (Double) event.get("gm1.growth");
			double diff = gm2Growth - gm1Growth;

			log.info("Diff: " + event.get("gm2.key") + " - " + event.get("gm1.key") + " = " + diff);
		});

		epStatement.start();
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
				StockEvent stockEvent = new StockEvent();
				stockEvent.key = key;
				stockEvent.closing = Double.parseDouble(record.get("Close"));
				stockEvent.date = format.parse(record.get("Date"));
				esper.sendEvent(stockEvent);

				Thread.sleep(r.nextInt(200));
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Logger log = LoggerFactory.getLogger(Main.class);

		Configuration esperClientConfiguration = new Configuration();
		EPServiceProvider esperServiceProvider = EPServiceProviderManager.getDefaultProvider(esperClientConfiguration);
		EPRuntime esperRuntime = esperServiceProvider.getEPRuntime();

		setupGrowthQuery(esperServiceProvider, log);
		setupComparyStocksQuery(esperServiceProvider, log);

		new Thread(() -> streamToEsper(esperRuntime, "yahoo-finance-cisco.csv", "cisco")).start();
		new Thread(() -> streamToEsper(esperRuntime, "yahoo-finance-apple.csv", "apple")).start();
		new Thread(() -> streamToEsper(esperRuntime, "yahoo-finance-ibm.csv", "ibm")).start();
	}
}

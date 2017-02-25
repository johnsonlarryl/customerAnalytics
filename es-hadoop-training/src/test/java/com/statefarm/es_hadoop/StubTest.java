package com.statefarm.es_hadoop;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class StubTest {
	private List<OutputMapRecord> outputMapRecords;
	
	private List<LongWritable> reducerShuffleValues1;
	private List<LongWritable> reducerShuffleValues2;
	
	private String word1;
	private String word2;
	
	private String[] records;
	
	private Text textword1;
	private Text textword2;
	
	private LongWritable countOutput1;
	private LongWritable countOutput2; 
	
  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
  ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
  
  /**
   * 	Sample Data Set
   * 	---------------
    	word	year	occ		pages	books
    	----	----	---		-----	-----
    	
    	dobbs   2007    20      18      15
   		dobbs   2008    22      20      12
		doctor  2007    545525  366136  57313
		doctor  2008    668666  446034  7269
   */

  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {
	  outputMapRecords = new ArrayList<OutputMapRecord>();
	  
	  word1 = "dobbs";
	  word2 = "doctor";
	  
	  // Java Fun Fact: According to the Java spec you cannot initial Array Instance members unless you 
	  // instantiate the Array Object with the new reserved word and the type of array you are initializing
	  records = new String[]{"dobbs	2007	20		18		15",
	  		  	 			 "doctor	2007	545525	366136	57313",
	  		  	 			 "dobbs	2008	22		20		12",
				 			 "doctor	2008	668666	446034	7269"};
	  
	  
		 
	  OutputMapRecord outputRecord1 = new OutputMapRecord(word1, 20L);
	  OutputMapRecord outputRecord2 = new OutputMapRecord(word2, 545525L);
	  OutputMapRecord outputRecord3 = new OutputMapRecord(word1, 22L);
	  OutputMapRecord outputRecord4 = new OutputMapRecord(word2, 668666L);
	  
	  
	  outputMapRecords.add(outputRecord1);
	  outputMapRecords.add(outputRecord2);
	  outputMapRecords.add(outputRecord3);
	  outputMapRecords.add(outputRecord4);
	  
	  reducerShuffleValues1 = new ArrayList<LongWritable>();
	  reducerShuffleValues2 = new ArrayList<LongWritable>();
	  
	  reducerShuffleValues1.add(new LongWritable(outputRecord1.getOccurrence()));
	  reducerShuffleValues1.add(new LongWritable(outputRecord3.getOccurrence()));
	  
	  reducerShuffleValues2.add(new LongWritable(outputRecord2.getOccurrence()));
	  reducerShuffleValues2.add(new LongWritable(outputRecord4.getOccurrence()));
	  
	  textword1 = new Text(word1);
	  textword2 = new Text(word2);
	  
	  countOutput1 = new LongWritable(42);
	  countOutput2 = new LongWritable(1214191);
	  

    /*
     * Set up the mapper test harness.
     */
    StubMapper mapper = new StubMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    StubReducer reducer = new StubReducer();
    reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
    reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {
	  try {
		  // Question: What is Long Writable as the first argument on the withInput method?
		  for (int record = 0; record < outputMapRecords.size(); record++) {
			  mapDriver.withInput(new LongWritable(), new Text(records[record]));
			  mapDriver.withOutput(new Text(outputMapRecords.get(record).getWord()), new LongWritable(outputMapRecords.get(record).getOccurrence()));
			  mapDriver.runTest();
		  }
	  } catch (IOException e) {
			fail("Mapper Unit Test Case failed: " + e.getMessage());
	  }
  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {
	  try {
		  reduceDriver.withInput(textword1, reducerShuffleValues1);
		  reduceDriver.withOutput(textword1, countOutput1);
		  reduceDriver.runTest();
		  
		  reduceDriver.withInput(textword2, reducerShuffleValues2);
		  reduceDriver.withOutput(textword2, countOutput2);
		  reduceDriver.runTest();
	  } catch (Exception e) {
		  fail("Reducer Unit Test Case failed: " + e.getMessage());
	  }
  }


  /*
   * Test the mapper and reducer working together.
   */
  @Ignore
  public void testMapReduce() {

	  try {
		  // TODO - Need to figure this out...need to come back to.
		  // Left off figuring what values to add to the 
		  for (int i = 0; i < records.length; i++) {
			  // Again, what the heck is a LongWritable doing here??;
			  mapReduceDriver.withInput(new LongWritable(i),  new Text(records[i])); 
			  mapReduceDriver.withOutput(textword1, countOutput2);
			  mapReduceDriver.withOutput(textword2, countOutput2);
		  
			  mapReduceDriver.runTest(true);
		  }
	  } catch (Exception e) {
		  fail("Map Reduce Unit Test Case failed: " + e.getMessage());
	  }
	  
	  
  }
  
  class OutputMapRecord {
	  private String word;
	  private Long occurrence;
	  
	  public OutputMapRecord(String word, Long occurrence) {
		  this.word = word;
		  this.occurrence = occurrence;
	  }
	  
	  public String getWord() {
		return word;
	  }
	  
	  public Long getOccurrence() {
		return occurrence;
	  }
  }
}

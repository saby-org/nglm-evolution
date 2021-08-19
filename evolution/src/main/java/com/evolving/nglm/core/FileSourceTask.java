package com.evolving.nglm.core;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde.PackSchema;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.evolution.UpdateChildrenRelationshipEvent;
import com.evolving.nglm.evolution.UpdateParentRelationshipEvent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.GZIPInputStream;

public abstract class FileSourceTask extends SourceTask {

	private static final Logger log = LoggerFactory.getLogger(FileSourceTask.class);

	protected abstract List<KeyValue> processRecord(String record) throws FileSourceTaskException, InterruptedException;
	@Override public String version() { return FileSourceConnector.FileSourceVersion; }

	//  configuration
	private String connectorName = null;
	private File directory = null;
	private int pollMaxRecords;
	private Charset fileCharset = null;
	private String topic = null;
	private Map<String, String> recordTopics = null;
	private String errorTopic = null;
	private File archiveDirectory = null;
	private int taskNumber;

	//  state
	private String taskName;
	private volatile boolean stopRequested = false;
	private final AtomicBoolean stopRequestedReference = new AtomicBoolean(false);
	private volatile boolean rebalanceRequested = false;
	private KafkaConsumer<byte[], byte[]> consumer = null;
	private final Object consumerLock = new Object();
	private final ConnectSerde<StringValue> stringValueSerde = StringValue.serde();

	public int getTaskNumber() { return taskNumber; }
	protected String getCurrentFilename() { return currentFile != null ? currentFile.getName() : "unknown"; }
	protected boolean getStopRequested() { return stopRequested; }
	protected String getRecordTopic(String recordType) { return recordTopics.get(recordType); }

	private volatile SourceFile currentFile = null;
	private BufferedReader reader = null;

	//  file lifecycle
	private final List<SourceFile> filesToProcess = new ArrayList<>();
	private final Set<SourceFile> processedFiles = new HashSet<>();
	// in flight records waiting ack of real completion (produced to kafka topic)
	private final Set<SourceRecord> inFlightRecords = new HashSet<>(pollMaxRecords);
	// list of action to do once records processed, before our internal "commit" call (save position state to kafka topic)
	private final List<PreCommitAction> preCommitActions = new ArrayList<>();


	// on start/rebalance, this contains the files that need to be restarted at a position
	private Map<TopicPartition, SourceFile> filesNeedToRestart = new HashMap<>();

	@Override
	public void start(Map<String, String> taskConfig) {

		connectorName = taskConfig.get("connectorName");
		taskNumber = parseIntegerConfig(taskConfig.get("taskNumber"));
		taskName = connectorName+"-task-"+taskNumber;
		log.info("{} -- Task.start() START", taskName);

		directory = new File(taskConfig.get("directory"));
		pollMaxRecords = parseIntegerConfig(taskConfig.get("pollMaxRecords"));
		fileCharset = Charset.forName(taskConfig.get("fileCharset"));
		topic = taskConfig.get("topic");
		archiveDirectory = (taskConfig.get("archiveDirectory").trim().length() > 0) ? new File(taskConfig.get("archiveDirectory")) : null;

		//  recordTopics
		//  - format is "record1:topic1,record2:topic2,..."
		log.info("topic {}", topic);
		recordTopics = new HashMap<>();
		if (topic.contains(":")) {
			String[] recordTopicTokens;
			try {
				recordTopicTokens = topic.split("[,]");
			} catch (PatternSyntaxException e) {
				throw new ServerRuntimeException(e);
			}
			for (String recordTopicToken : recordTopicTokens) {
				log.info("processing {}", recordTopicToken);
				String[] recordTopicPair;
				try {
					recordTopicPair = recordTopicToken.split("[:]", 2);
				} catch (PatternSyntaxException e) {
					throw new ServerRuntimeException(e);
				}
				if (recordTopicPair.length > 0) {
					log.info("recordTopicPair: ", recordTopicPair);
					recordTopics.put(recordTopicPair[0], recordTopicPair[1]);
				}
			}
			topic = null;
		}
		//  errorTopic
		errorTopic = recordTopics.get("error");

		// set up consumer
		String bootstrapServers = taskConfig.get("bootstrapServers");
		String internalTopic = taskConfig.get("internalTopic");

		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", bootstrapServers);
		consumerProperties.put("group.id", "fileconnector-" + connectorName);
		consumerProperties.put("auto.offset.reset", "earliest");
		consumerProperties.put("enable.auto.commit", "false");
		consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 180000);
		consumerProperties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 180000);
		consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 180000);
		consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
		consumer = new KafkaConsumer<>(consumerProperties);
		ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
			@Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { partitionsRevoked(partitions); }
			@Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { partitionsAssigned(partitions); }
		};
		consumer.subscribe(Arrays.asList(internalTopic), listener);

	}

	@Override
	public void stop() {

		log.info("{} -- Task.stop() called", taskName);

		synchronized (this){

			stopRequested = true;
			stopRequestedReference.set(true);

			stopStatisticsCollection();

			this.notifyAll();

		}

		// if we block stop() call, we block sendRecords() in upper class (deadlock as we wait for record to be sent)
		// so we move away the final commit if needed (last poll() returned results, but is never call again by upper class) in a thread
		new Thread(this::finish,taskName+"-finisher").start();

		log.info("{} -- Task.stop() done", taskName);

	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {

		log.debug("{} -- poll START", taskName);

		// check if we need to wait records to be ack well pushed to kafka
		waitInflightRecordsAndCommitProgress();

		List<SourceRecord> result = nextSourceRecords();

		synchronized (this){
			// pause if nothing to do, but not infinitely, this to avoid "pause task" call to hang
			if (result == null && !stopRequested && !rebalanceRequested) {
				try { this.wait(3000); } catch (InterruptedException e) {}
			}else if(stopRequested || rebalanceRequested){
				if(rebalanceRequested) rebalanceRequested=false;
				inFlightRecords.clear();
				preCommitActions.clear();
				result=null;
			}else{
				inFlightRecords.addAll(result);
			}
		}

		log.debug("{} -- poll returning {} records", taskName, (result!=null?result.size():0));
		return result;
	}

	private synchronized void waitInflightRecordsAndCommitProgress(){
		while(!inFlightRecords.isEmpty()){
			log.trace("{} -- poll still waiting {} records to be acks", taskName, inFlightRecords.size());
			try{this.wait(2000/*for periodic logging, but we should not rely on that delay for unblocking*/);}catch (InterruptedException e){}
		}
		commitProgress();
	}

	private synchronized void finish(){

		// connect seems fully hide exception there
		Thread.UncaughtExceptionHandler previous = Thread.currentThread().getUncaughtExceptionHandler();
		Thread.currentThread().setUncaughtExceptionHandler((thread,exception)->{
			log.warn("exception while finish",exception);
			if(previous!=null) previous.uncaughtException(thread,exception);
		});

		log.info("{} -- finish start", taskName);

		waitInflightRecordsAndCommitProgress();

		if(consumer!=null){
			log.info("{} -- finish closing internal consumer", taskName);
			consumer.close();
			consumer=null;
		}
		log.info("{} -- finish end", taskName);
	}

	@Override
	public synchronized void commitRecord(SourceRecord record) throws InterruptedException {
		inFlightRecords.remove(record);
		if(inFlightRecords.isEmpty()) this.notifyAll();
		super.commitRecord(record);
	}

	// the real files read jobs
	private List<SourceRecord> nextSourceRecords() throws InterruptedException {

		List<SourceRecord> result = null;

		while (true) {
			synchronized (this){

				if(stopRequested||rebalanceRequested) return null;

				// read next record from currentFile

				if (currentFile != null) {

					String record;
					try {
						record = reader.readLine();
						preCommitActions.add(new IncrementFilePosition(currentFile));
					} catch (IOException e) {
						log.info(taskName+" -- nextSourceRecords bufferedReader", e);
						record = null;
					}
					// end of file (or IO error)
					if (record == null) {
						log.debug("{} -- nextSourceRecords closing processed file {}", taskName, currentFile.getName());
						closeCurrentFile();
						updateFilesProcessed(connectorName, taskNumber, 1);
						continue;
					}

					if (record.trim().length() == 0) continue;

					//  parse
					List<KeyValue> recordResults;
					try {
						recordResults = processRecord(record.trim());// custom implementation call
					} catch (InterruptedException e) {
						throw e;//not an error
					}
					catch (Exception e)// we usually avoid crash on custom code calls
					{
						updateErrorRecords(connectorName, taskNumber, 1);
						// if FileSourceTaskException, that is something expected by custom code, so otherwise is probably bad error
						if (!(e instanceof FileSourceTaskException))
							log.error(taskName+" -- nextSourceRecords unexpected exception processing record: " + record, e);
						if (errorTopic != null) {
							ErrorRecord errorRecord = new ErrorRecord(record.trim(), SystemTime.getCurrentTime(), currentFile.getName(), e.getMessage());
							recordResults = Collections.singletonList(new KeyValue("error", null, null, ErrorRecord.schema(), ErrorRecord.pack(errorRecord)));
						} else {
							continue;
						}
					}

					// create a KeyValue event for the parent if needed
					ArrayList<KeyValue> eventToAdd = null;
					for (KeyValue recordResult : recordResults) {
						//  hierarchy relationship event generation for the parent
						Object eventObject = recordResult.getValue();

						if (recordResult instanceof KeyValueWithParentUpdate && ((KeyValueWithParentUpdate) recordResult).getUpdateParentRelationshipEvent() != null) {
							KeyValueWithParentUpdate keyValueWithParentUpdate = (KeyValueWithParentUpdate) recordResult;
							UpdateParentRelationshipEvent parentRelationEvent = keyValueWithParentUpdate.getUpdateParentRelationshipEvent();
							// need to create an event to the parent if this one exists
							// but also update the event newParent with the parent's subscriberID
							if (parentRelationEvent.getNewParent() == null || parentRelationEvent.getNewParent().equals("")) {
								continue; /* the event is not used for hierarchy update */
							}
							String parentSubscriberID = resolveSubscriberID(parentRelationEvent.getNewParentAlternateIDName(), parentRelationEvent.getNewParent());
							if (parentSubscriberID != null) {
								// update original event with internal subscriberID as newParent
								parentRelationEvent.setNewParent(parentSubscriberID);
								keyValueWithParentUpdate.setValue(keyValueWithParentUpdate.getPackSchema().pack(parentRelationEvent));

								// generate an event for the parent
								UpdateChildrenRelationshipEvent updateChildrenRelationshipEvent =
										new UpdateChildrenRelationshipEvent(
												parentSubscriberID,
												SystemTime.getCurrentTime(),
												parentRelationEvent.getRelationshipDisplay(),
												parentRelationEvent.getSubscriberID(),
												parentRelationEvent.isDeletion());
								KeyValue keyValue = new KeyValue(
										recordResult.getRecordType(),
										Schema.STRING_SCHEMA,
										parentSubscriberID,
										UpdateChildrenRelationshipEvent.schema(),
										UpdateChildrenRelationshipEvent.pack(updateChildrenRelationshipEvent));
								if (eventToAdd == null) {
									eventToAdd = new ArrayList<>();
								}
								eventToAdd.add(keyValue);
							} else {
								log.warn("{} -- nextSourceRecords can't retrieve Parent information {} : {}", taskName, parentRelationEvent.getNewParentAlternateIDName(), parentRelationEvent.getNewParent());
								parentRelationEvent.setNewParent(parentSubscriberID);
								keyValueWithParentUpdate.setValue(keyValueWithParentUpdate.getPackSchema().pack(parentRelationEvent));
							}
						}
					}
					if (eventToAdd != null) {
						recordResults = new ArrayList<>(recordResults); // to avoid an UnsupportedOperationException
						recordResults.addAll(eventToAdd);
					}

					for (KeyValue recordResult : recordResults) {

						//  topic
						String recordTopic = (recordResult.getRecordType() != null) ? recordTopics.get(recordResult.getRecordType()) : topic;
						if (recordTopic == null) throw new ServerRuntimeException("cannot determine topic for record type " + recordResult.getRecordType());

						if(result==null) result = new ArrayList<>();
						int recordNumber = 0;
						//  sourceRecord
						result.add(new SourceRecord(/*sourcePartition*/null,/* sourceOffset*/null, recordTopic, recordResult.getKeySchema(), recordResult.getKey(), recordResult.getValueSchema(), recordResult.getValue()));
						// stats
						recordNumber += 1;
						updateRecordStatistics(connectorName, taskNumber, recordTopic, recordNumber);
					}

					if(result.size()+inFlightRecords.size() >= pollMaxRecords) break;//enough records, return them
					continue; // otherwise continue

				}

				// read next file

				// poll queue if empty
				if(filesToProcess.isEmpty()) pollJobs();

				SourceFile fileToProcess ;
				fileToProcess = (filesToProcess.size() > 0) ? filesToProcess.remove(0) : null;

				if (fileToProcess != null) {

					currentFile = fileToProcess;
					log.debug("{} -- nextSourceRecords fileToProcess: {}", taskName, currentFile.getAbsolutePath());

					//  ensure currentFile still exists
					if (!currentFile.exists()) {
						log.warn("{} -- nextSourceRecords fileToProcess does not exists {}", taskName, currentFile.getAbsolutePath());
						closeCurrentFile();
						continue;
					}

					try {

						if (fileToProcess.getLineProcessed()==0) {
							// new file
							log.info("{} -- nextSourceRecords fileToProcess {}", taskName, currentFile.getAbsolutePath());
						} else {
							// restart of a file (rebalance, stop,...)
							log.info("{} -- nextSourceRecords fileToProcess (restart) {} at line {}", taskName, currentFile.getAbsolutePath(), currentFile.getLineProcessed());

						}

						//  gzip format?
						Pattern gzipPattern = Pattern.compile("^.*\\.(.*)$");
						Matcher gzipMatcher = gzipPattern.matcher(currentFile.getAbsolutePath());
						boolean gzipFormat = gzipMatcher.matches() && gzipMatcher.group(1).trim().toLowerCase().equals("gz");
						if(gzipFormat) log.debug("{} -- nextSourceRecords {} will be open as gzip file", taskName, currentFile.getAbsolutePath());
						//  input stream w/ gzip support
						FileInputStream fileInputStream = new FileInputStream(currentFile.getAbsolutePath());
						InputStream inputStream = gzipFormat ? new GZIPInputStream(fileInputStream) : fileInputStream;
						//  restart reader, at right line if needed
						reader = new BufferedReader(new InputStreamReader(inputStream, fileCharset));
						for (long i = 0; i < currentFile.getLineProcessed(); i++) {
							String record = reader.readLine();
							if (record == null) { // full file actually processed
								closeCurrentFile();
								break;
							}
						}

					} catch (IOException e) {
						log.info(taskName+" -- nextSourceRecords IO issue", e);
						closeCurrentFile();
					}

					continue;
				}

				// no result and no files available -- break and return
				break;

			}
		}
		return result;
	}

	private synchronized void closeCurrentFile(){
		if (reader != null) try { reader.close(); } catch (IOException e1) {}
		preCommitActions.add(new AddToProcessedFiles(currentFile));
		currentFile = null;
		reader = null;
	}

	public synchronized void commitProgress() {

		synchronized (consumerLock){

			if(consumer==null) return;// commit called after stopped done

			log.debug("{} -- commit START", taskName);

			// apply all actions pending
			for(PreCommitAction action:preCommitActions) action.onCommit();
			preCommitActions.clear();
			inFlightRecords.clear();

			// fully processed files
			int nbFilesFullyCommit=0;
			Map<TopicPartition,OffsetAndMetadata> toCommit = new HashMap<>();
			for(SourceFile processed:processedFiles){
				// delete/archive files as necessary
				if (processed.exists()) {
					try {
						boolean successfullyRemoved = (archiveDirectory != null) ? processed.getFile().renameTo(new File(archiveDirectory, processed.getName())) : processed.getFile().delete();
						if (!successfullyRemoved) {
							log.error("{} -- commit COULD NOT REMOVE FILE {}", taskName, processed.getAbsolutePath());
						}
					} catch (SecurityException e) {
						log.error(taskName+" -- exception, commit COULD NOT REMOVE FILE"+processed.getAbsolutePath(), e);
					}
				}
				// check offsets that need to be commited
				OffsetAndMetadata offsetAndMetadataToCommit = toCommit.get(processed.getPartition());
				if(offsetAndMetadataToCommit==null || processed.getOffset()+1>offsetAndMetadataToCommit.offset()) toCommit.put(processed.getPartition(), new OffsetAndMetadata(processed.getOffset()+1));
				nbFilesFullyCommit++;
			}
			processedFiles.clear();

			// current file, saving line position in the file into offsetMetadata (commit then at same offset, not increasing position on partition)
			String logFileInProgress = "";
			if(currentFile!=null){
				OffsetAndMetadata offsetAndMetadataToCommit = toCommit.get(currentFile.getPartition());
				// just an extra check, should not happen
				if(offsetAndMetadataToCommit!=null && offsetAndMetadataToCommit.offset()>currentFile.getOffset()){
					log.error("{} -- commit inconsistency with current file {} at offset {}, already to commit {}", taskName, currentFile.getAbsolutePath(), currentFile.getLineProcessed(), offsetAndMetadataToCommit);
				}
				// DO NOT INCREASE POSITION, COMMIT SAME OFFSET
				logFileInProgress = ", file in progress "+currentFile.getAbsolutePath()+" processed line "+currentFile.getLineProcessed();
				toCommit.put(currentFile.getPartition(),new OffsetAndMetadata(currentFile.getOffset(),currentFile.toJsonMetadata()));
			}

			if(!toCommit.isEmpty()) consumer.commitSync(toCommit);

			log.debug("{} -- commit done, {} files completed{}", taskName, nbFilesFullyCommit, logFileInProgress);

		}

	}

	private void pollJobs() {

		ConsumerRecords<byte[], byte[]> records;
		try {
			synchronized (consumerLock){
				records = consumer.poll(Duration.ofSeconds(2));
			}
		} catch (WakeupException e) {
			records = ConsumerRecords.empty();
		}

		//  update filesToProcess
		for (ConsumerRecord<byte[], byte[]> record : records) {
			SourceFile sourceFile = new SourceFile(record);
			log.debug("{} -- runConsumer adding file {}", taskName, sourceFile.getAbsolutePath());
			filesToProcess.add(sourceFile);
		}

	}

	private synchronized void partitionsRevoked(Collection<TopicPartition> revokedPartitions) {

		log.info("{} -- rebalanceRequested partitionsRevoked : {}", taskName, revokedPartitions);

		rebalanceRequested = true;// tell current poll to just fully abort

		if(!inFlightRecords.isEmpty()){
			// we are waiting the ack for those records, but internal consumer rebalance, so thats our last chance to commit, so we do
			// but if the commit position to kafka success, while in flights don't, we will loose them
			commitProgress();
		}else{
			// else we should be clean, we just abort the current poll on going
			for(PreCommitAction action:preCommitActions) action.onAbort();
		}

		filesToProcess.removeIf(sourceFile->revokedPartitions.contains(sourceFile.getPartition()));

		for(TopicPartition topicPartition:revokedPartitions) filesNeedToRestart.remove(topicPartition);

		log.info("{} -- rebalanceRequested partitionsRevoked done", taskName);
	}

	private synchronized void partitionsAssigned(Collection<TopicPartition> assignedPartitions){

		log.info("{} -- rebalanceRequested partitionsAssigned : {}", taskName, assignedPartitions);

		try{
			for(TopicPartition topicPartition:assignedPartitions){
				synchronized (consumerLock){
					OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
					if(offsetAndMetadata==null) continue;// not yet commit at all on this partition
					String metadata = offsetAndMetadata.metadata();

					//TODO uncomment this line once everyone run EVPRO-1189 :
					//if(metadata==null || metadata.isEmpty()) continue;//no file to restart for that partition
					//TODO remove once everyone run EVPRO-1189 :
					if(metadata==null || metadata.isEmpty()){
						filesNeedToRestart.put(topicPartition,null);
						continue;
					}
					// END TODO remove

					try{
						SourceFile restartFile = new SourceFile(metadata,topicPartition,offsetAndMetadata.offset());
						filesNeedToRestart.put(topicPartition,restartFile);
					}catch(NumberFormatException e){
						log.error("{} -- rebalanceRequested wrong metadata commited in partition {} : {}", taskName, topicPartition, metadata);
					}
				}
			}
		}catch (WakeupException e){
			log.info("{} -- rebalanceRequested consumer wakeup", taskName);
		}

		log.info("{} -- rebalanceRequested partitionsAssigned done", taskName);
	}

	private abstract class PreCommitAction{
		SourceFile sourceFile;
		PreCommitAction(SourceFile sourceFile){this.sourceFile=sourceFile;}
		public abstract void onCommit();
		public abstract void onAbort();
	}
	private class IncrementFilePosition extends PreCommitAction {
		IncrementFilePosition(SourceFile sourceFile){super(sourceFile);}
		@Override public synchronized void onCommit() { sourceFile.incrementLineProcessed(); }
		@Override public synchronized void onAbort() {}
	}
	private class AddToProcessedFiles extends PreCommitAction {
		AddToProcessedFiles(SourceFile sourceFile){super(sourceFile);}
		@Override public synchronized void onCommit() { processedFiles.add(sourceFile); }
		@Override public synchronized void onAbort() { synchronized(filesToProcess){filesToProcess.add(sourceFile);} }
	}

	private class SourceFile {

		private File file;
		private TopicPartition partition;
		private long offset;
		private long lineProcessed;

		// call it only from kafka consumer
		private SourceFile(ConsumerRecord<byte[], byte[]> record) {
			StringValue filename = stringValueSerde.deserializer().deserialize(record.topic(), record.value());
			this.file = new File(directory, filename.getValue());
			this.partition = new TopicPartition(record.topic(), record.partition());
			this.offset = record.offset();

			// check if restart needed for this file
			SourceFile restart = filesNeedToRestart.get(this.partition);
			if(restart==null){
				this.lineProcessed = 0;

				//TODO remove this when everyone run EVPRO-1189
				if(filesNeedToRestart.containsKey(this.getPartition())){
					//OLD WAY OF STORING FILE PROGRESS
					Map<String, String> sourcePartition = new HashMap<>();
					sourcePartition.put("absolutePath", this.file.getAbsolutePath());
					Map<String, Object> sourceOffset = context.offsetStorageReader().offset(sourcePartition);
					if(sourceOffset!=null){
						Long restartLineNumber = (Long) sourceOffset.get("lineNumber");
						if(restartLineNumber!=null){
							this.lineProcessed = restartLineNumber;
							log.info("{} -- SourceFile restart post EVPRO-1189 migration but file {} was in progress at line {}", taskName, this.getAbsolutePath(), this.getLineProcessed());
						}
					}
					filesNeedToRestart.remove(this.partition);
				}
				// END TODO remove

			}else{
				if (!this.getAbsolutePath().equals(restart.getAbsolutePath())){
					log.warn("{} -- SourceFile saved restarted needed for {}, but file to process is {}", taskName, restart.getAbsolutePath(), this.getAbsolutePath());
					this.lineProcessed = 0;
				}else{
					this.lineProcessed = restart.lineProcessed;
				}
				filesNeedToRestart.remove(this.partition);
			}
		}

		public SourceFile(String jsonMetadata, TopicPartition topicPartition, long offset) {
			try {
				JSONObject json = (JSONObject) (new JSONParser()).parse(jsonMetadata);
				String absolutePath = JSONUtilities.decodeString(json,"file",true);
				this.lineProcessed = JSONUtilities.decodeLong(json,"line",true);
				this.file = new File(absolutePath);
				this.partition = topicPartition;
				this.offset = offset;
			} catch (ParseException e) {
				throw new JSONUtilities.JSONUtilitiesException(jsonMetadata+" is not json metadata");
			}
		}

		public File getFile() { return file; }
		public TopicPartition getPartition() { return partition; }
		public long getOffset() { return offset; }
		public long getLineProcessed() { return lineProcessed; }
		public String getName() { return file.getName(); }
		public String getAbsolutePath() { return file.getAbsolutePath(); }
		public boolean exists() { return file.exists(); }

		public void incrementLineProcessed() { this.lineProcessed++; }

		public String toJsonMetadata(){
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("file",getAbsolutePath());
			jsonObject.put("line",getLineProcessed());
			return jsonObject.toJSONString();
		}

	}


	// stats

	private Map<Integer, FileSourceTaskStatistics> allTaskStatistics = new HashMap<Integer, FileSourceTaskStatistics>();

	private FileSourceTaskStatistics getStatistics(String connectorName, int taskNumber) {
		synchronized (allTaskStatistics) {
			FileSourceTaskStatistics taskStatistics = allTaskStatistics.get(taskNumber);
			if (taskStatistics == null) {
				try {
					taskStatistics = new FileSourceTaskStatistics(connectorName, taskNumber);
				} catch (ServerException se) {
					throw new ServerRuntimeException("Could not create statistics object", se);
				}
				allTaskStatistics.put(taskNumber, taskStatistics);
			}
			return taskStatistics;
		}
	}

	private void updateFilesProcessed(String connectorName, int taskNumber, int amount) {
		synchronized (allTaskStatistics) {
			FileSourceTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
			taskStatistics.updateFilesProcessed(amount);
		}
	}

	private void updateErrorRecords(String connectorName, int taskNumber, int amount) {
		synchronized (allTaskStatistics) {
			FileSourceTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
			taskStatistics.updateErrorRecords(amount);
		}
	}

	private void updateRecordStatistics(String connectorName, int taskNumber, String recordType, int amount) {
		synchronized (allTaskStatistics) {
			FileSourceTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
			taskStatistics.updateRecordStatistics(recordType, amount);
		}
	}

	private void stopStatisticsCollection() {
		synchronized (allTaskStatistics) {
			for (FileSourceTaskStatistics taskStatistics : allTaskStatistics.values()) {
				taskStatistics.unregister();
			}
			allTaskStatistics.clear();
		}
	}

	public static class KeyValue {

		private String recordType;
		private Schema keySchema;
		private Object key;
		private Schema valueSchema;
		protected Object value;

		public KeyValue(String recordType, Schema keySchema, Object key, Schema valueSchema, Object value) {
			this.recordType = recordType;
			this.keySchema = keySchema;
			this.key = key;
			this.valueSchema = valueSchema;
			this.value = value;
		}

		public KeyValue(Schema keySchema, Object key, Schema valueSchema, Object value) {
			this.recordType = null;
			this.keySchema = keySchema;
			this.key = key;
			this.valueSchema = valueSchema;
			this.value = value;
		}

		public String getRecordType() { return recordType; }
		public Schema getKeySchema() { return keySchema; }
		public Object getKey() { return key; }
		public Schema getValueSchema() { return valueSchema; }
		public Object getValue() { return value; }

	}

	public static class KeyValueWithParentUpdate extends KeyValue {
		private UpdateParentRelationshipEvent updateParentRelationshipEvent;
		private PackSchema packSchema;

		public KeyValueWithParentUpdate(String recordType, Schema keySchema, Object key, Schema valueSchema, Object value, UpdateParentRelationshipEvent updateParentRelationshipEvent, PackSchema packSchema) {
			super(recordType, keySchema, key, valueSchema, value);
			this.updateParentRelationshipEvent = updateParentRelationshipEvent;
			this.packSchema = packSchema;
		}

		public UpdateParentRelationshipEvent getUpdateParentRelationshipEvent() { return updateParentRelationshipEvent; }
		public PackSchema getPackSchema() { return packSchema; }
		public void setValue(Object value) { this.value = value; }

	}

	public static class FileSourceTaskException extends Exception {
		public FileSourceTaskException(String message) { super(message); }
		public FileSourceTaskException(Throwable e) { super(e); }
		public FileSourceTaskException(String message, Throwable e) { super(message, e); }
	}


	// utils

	protected Integer parseIntegerConfig(String attribute) {
		try {
			return (attribute != null) ? Integer.parseInt(attribute) : null;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	protected String readString(String token, String record, String defaultValue) {
		return FileSourceConnector.readString(token, record, defaultValue);
	}
	protected String readString(String token, String record) {
		return FileSourceConnector.readString(token, record, null);
	}

	protected Integer readInteger(String token, String record, Integer defaultValue) throws IllegalArgumentException {
		return FileSourceConnector.readInteger(token, record, defaultValue);
	}
	protected Integer readInteger(String token, String record) throws IllegalArgumentException {
		return FileSourceConnector.readInteger(token, record, null);
	}

	protected Long readLong(String token, String record, Long defaultValue) throws IllegalArgumentException {
		return FileSourceConnector.readLong(token, record, defaultValue);
	}
	protected Long readLong(String token, String record) throws IllegalArgumentException {
		return FileSourceConnector.readLong(token, record, null);
	}

	protected Boolean readBoolean(String token, String record, Boolean defaultValue) throws IllegalArgumentException {
		return FileSourceConnector.readBoolean(token, record, defaultValue);
	}
	protected Boolean readBoolean(String token, String record) throws IllegalArgumentException {
		return FileSourceConnector.readBoolean(token, record, null);
	}

	protected Date readDate(String token, String record, String format, String timeZone, Date defaultValue) throws IllegalArgumentException {
		return FileSourceConnector.readDate(token, record, format, timeZone, defaultValue);
	}
	protected Date readDate(String token, String record, String format, String timeZone) throws IllegalArgumentException {
		return FileSourceConnector.readDate(token, record, format, timeZone, null);
	}
	protected Date readDate(String token, String record, String format, Date defaultValue, int tenantID) throws IllegalArgumentException {
		return FileSourceConnector.readDate(token, record, format, Deployment.getDeployment(tenantID).getTimeZone(), defaultValue);
	}
	protected Date readDate(String token, String record, String format, int tenantID) throws IllegalArgumentException {
		return FileSourceConnector.readDate(token, record, format, Deployment.getDeployment(tenantID).getTimeZone(), null);
	}

	protected Double readDouble(String token, String record, Double defaultValue) throws IllegalArgumentException {
		return FileSourceConnector.readDouble(token, record, defaultValue);
	}
	protected Double readDouble(String token, String record) throws IllegalArgumentException {
		return FileSourceConnector.readDouble(token, record, null);
	}


	// utils resolveSubscriberID

	// singleton lazy init holder
	private static class SubscriberIDServiceHolder {
		private static SubscriberIDService INSTANCE = new SubscriberIDService(Deployment.getRedisSentinels(), "connect-file-source", true);
	}
	private SubscriberIDService getSubscriberIDService() {
		return SubscriberIDServiceHolder.INSTANCE;
	}

	// child subscriber id resolver
	protected Pair<String, Integer> resolveSubscriberIDAndTenantID(String alternateIDName, String alternateID) throws InterruptedException {
		try {
			return getSubscriberIDService().getSubscriberIDAndTenantIDBlocking(alternateIDName, alternateID, stopRequestedReference);
		} catch (SubscriberIDServiceException e) {
			log.error(this.taskName + " resolveSubscriberID exception", e);
			// seems bad error if this happens, safer to crash
			throw new RuntimeException(e);
		}
	}
	// derived
	protected String resolveSubscriberID(String alternateIDName, String alternateID) throws InterruptedException {
		Pair<String,Integer> subscriberIDAndTenantID = resolveSubscriberIDAndTenantID(alternateIDName,alternateID);
		if(subscriberIDAndTenantID==null) return null;
		return subscriberIDAndTenantID.getFirstElement();
	}

}

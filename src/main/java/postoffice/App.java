/*
 * Postoffice: Simple threaded messaging using Cassandra.
 * Copyright (C) 2011 Hisham Mardam-Bey <hisham.mardambey@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package postoffice;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.jboss.netty.channel.Channels.pipeline;
import static org.jboss.netty.handler.codec.http.HttpHeaders.getHost;
import static org.jboss.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLogLevel;
import org.jboss.netty.util.CharsetUtil;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.UuidHelper;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.IThriftPool;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Postoffice is a simple threaded messaging system on top of Cassandra.
 * 
 * The system can either be accessed using the Java API or via an HTTP 
 * interface that returns JSON.
 * 
 * Example HTTP requests:
 * 
 * Get first 10 messages in a the "inbox" folder for "1501572":
 * http://braindump:8081/folder/?folder=inbox&owner=1501572&start=0&count=10
 * 
 * Create a new conversation:
 * http://braindump:8081/new/?from=1501572&to=1501571&subject=hey&body=foobarbaz
 * 
 * Reply to an existing conversation given its id:
 * http://braindump:8081/reply/?from=1501571&to=1501572&subject=hey&body=foobarbazreply&id=218779d1-ac51-11e0-8616-005056c00008
 * 
 * Cassandra schema:
 * 
 * create column family folders with column_type = 'Standard' and comparator = 'TimeUUIDType';
 * create column family conversations with column_type = 'Standard' and comparator = 'TimeUUIDType';
 * 
 * @author Hisham Mardam-Bey
 *
 */

public class App
{		
	public final static Integer HTTPD_PORT = 8081;
	
	public static void startWebInterface()
	{						
		try
		{
			HttpServer httpd = new HttpServer(HTTPD_PORT);
			httpd.start();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
//			httpd = new Httpd(HTTPD_HOST, HTTPD_PORT);
//			httpd.handle("/folder", FolderHandler.get());
//			httpd.handle("/new", NewConversationHandler.get());
//			httpd.handle("/reply", ReplyHandler.get());
//			httpd.handle("/", IndexHandler.get());
//			httpd.addFinalizeHook(new Runnable() 
//			{
//				public void run()
//				{
//		    		PelopsUtil.disconnect();
//				}
//			});
//			httpd.start();
//		}
		//catch(IOException e)
		//{
		//	e.printStackTrace();
		//}
	}
	
	public static void populateData()
	{
    	String alice = "1501571";
    	String bob = "1501572";
    	
    	final String from = alice;
    	final String to = bob;
    	
//    	for (int t = 0; t < 20; t++)
//    	{
//    		new Thread() {
//    			public void run()
//    			{
    				for (int i = 0; i < 250; i++)
    		    	{
    			    	PostofficeUtil.startConversation(
    			    	  from, to, 
    			    	  "Postoffice project! " + System.nanoTime(), 
    			    	  "Hi " + to + ", I just wanted to tell you about Postoffice, a messaging system using Cassandra! (= Best, " + from
    			    	);
    		    	}
//    			}
//    		}.start();
//    	}
    	
	}
	
    public static void main( String[] args ) throws Exception
    {    
    	PelopsUtil.connect();
    
    	if (args.length > 0 && args[0].equals("--populate"))
    	{
    		System.out.println("Populating data between 1501571 and 1501572");
    		populateData();
    	}
    	    	
    	startWebInterface();  
    }        
}

/**
 * A conversation is a sorted set of messages (by creation time).
 * 
 * @author hisham
 *
 */
class Conversation implements Comparable<Conversation>
{
	protected String m_strId;
	protected TreeSet<Message> m_setMessages;
	protected Long m_lLastReceivedDate;
	
	public Conversation()
	{		
	}
	
	public Conversation(String strId)
	{
		m_strId = strId;
	}
	/**
	 * @return the lastReceivedDate
	 */
	public Long getLastReceivedDate()
	{
		return m_lLastReceivedDate;
	}
	/**
	 * @param lastReceivedDate the lastReceivedDate to set
	 */
	public void setLastReceivedDate(Long lastReceivedDate)
	{
		m_lLastReceivedDate = lastReceivedDate;
	}
	/**
	 * @return the m_strId
	 */
	public String getId()
	{
		return m_strId;
	}
	/**
	 * @param strId the m_strId to set
	 */
	public void setId(String strId)
	{
		m_strId = strId;
	}
	/**
	 * @return the m_setMessages
	 */
	public TreeSet<Message> getMessages()
	{
		return m_setMessages;
	}
	/**
	 * @param setMessages the m_setMessages to set
	 */
	public void setMessages(TreeSet<Message> setMessages)
	{
		m_setMessages = setMessages;
	}
	
	@Override
	public int compareTo(Conversation c)
	{
		return c.getLastReceivedDate().compareTo(m_lLastReceivedDate);
	}
}

class Message implements Comparable<Message>
{
	protected String m_strId;
	protected String m_strSubject;
	protected String m_strBody;
	protected String m_strSender;
	
	public Message(String strId, String strSender, String strSubject, String strBody)
	{
		m_strId = strId;
		m_strSender = strSender;
		m_strSubject = strSubject;
		m_strBody = strBody;
	}
	
	/**
	 * @return the id
	 */
	public String getId()
	{
		return m_strId;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id)
	{
		m_strId = id;
	}		
	
	/**
	 * @return the subject
	 */
	public String getSubject()
	{
		return m_strSubject;
	}
	/**
	 * @param subject the subject to set
	 */
	public void setSubject(String subject)
	{
		m_strSubject = subject;
	}
	/**
	 * @return the body
	 */
	public String getBody()
	{
		return m_strBody;
	}
	/**
	 * @param body the body to set
	 */
	public void setBody(String body)
	{
		m_strBody = body;
	}
	/**
	 * @return the sender
	 */
	public String getSender()
	{
		return m_strSender;
	}
	/**
	 * @param sender the sender to set
	 */
	public void setSender(String sender)
	{
		m_strSender = sender;
	}
	
	@Override
	public int compareTo(Message m)
	{
		return m.getId().compareTo(m_strId);
	}
}

class Folder
{
	protected String m_strName;
	protected String m_strOwner;
	protected Set<Conversation> m_setConvs;
		
	/**
	 * @return the conversations
	 */
	public Set<Conversation> getConversations()
	{
		return m_setConvs;
	}

	public void setConversations(Set<Conversation> setConvs)
	{
		m_setConvs = setConvs;
	}

	public Folder(String strName, String strOwner)
	{
		m_strName = strName;
		m_strOwner = strOwner;	
	}
	
	/**
	 * @return the name
	 */
	public String getName()
	{
		return m_strName;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name)
	{
		m_strName = name;
	}
	/**
	 * @return the owner
	 */
	public String getOwner()
	{
		return m_strOwner;
	}
	/**
	 * @param owner the owner to set
	 */
	public void setOwner(String owner)
	{
		m_strOwner = owner;
	}		
}

class FolderUtil
{
	protected static Gson gson = new Gson();
	public static final String FOLDERS = "folders";
	protected static String pool = "pool";
	
	public static boolean addConversation(Folder f, Conversation c)
	{
		try
		{
        	Mutator mutator = PelopsUtil.getPool().createMutator();
        	_addConvToFolder(mutator, f, c);        	        		        
        	mutator.execute(ConsistencyLevel.ONE);
        	return true;
		}
		catch (Exception e)
		{
			return false;
		}
	}	
	
	public static String getId(Folder f)
	{
		return f.getOwner() + PostofficeUtil.DELIM + f.getName();
	}
	
	public static Set<Conversation> getConversations(Folder f, Long lStart, Integer iCount)
	{		
    	Selector selector = PelopsUtil.getPool().createSelector();
    	SlicePredicate pred = new SlicePredicate();
    	SliceRange sliceRange = new SliceRange();
    	sliceRange.setStart("".getBytes());
    	sliceRange.setFinish("".getBytes());
    	sliceRange.setCount(lStart.intValue() + iCount);
    	sliceRange.setReversed(true);
    	
    	pred.setSlice_range(sliceRange);
    	
    	List<Column> listConvs = selector.getColumnsFromRow(FOLDERS, getId(f), pred, ConsistencyLevel.ONE);
    	
    	// page list if needed
    	if (lStart != 0)
    	{
    		Integer iEnd = lStart.intValue() + iCount;
    		
    		// only get as many elements as the folder has
    		if (iEnd > listConvs.size() - 1)
    		{
    			iEnd = listConvs.size() - 1;
    		}
    		
    		// if asked to start after the max size 
    		// of the folder bail out
    		if (lStart > listConvs.size() - 1)
    		{
    			return new TreeSet<Conversation>();
    		}
    		
    		listConvs = listConvs.subList(lStart.intValue(), iEnd);
    	}
    	
    	List<Bytes> listConvIds = new ArrayList<Bytes>();
    	
    	for (Column col: listConvs)
    	{
    		// the value of the column is the conversation row's key
    		listConvIds.add(Bytes.fromUTF8(Selector.getColumnStringValue(col)));
    	}
    	
    	if (listConvs.size() == 0)
    	{
    		return new TreeSet<Conversation>();
    	}
    	    	
    	pred = new SlicePredicate();
    	sliceRange = new SliceRange();
    	sliceRange.setStart("".getBytes());
    	sliceRange.setFinish("".getBytes());    	
    	pred.setSlice_range(sliceRange);
    	
    	LinkedHashMap<Bytes, List<Column>> mapConvs = selector.getColumnsFromRows(ConversationUtil.CONVERSATIONS, listConvIds, pred, ConsistencyLevel.ONE);
    	TreeSet<Message> setMessages;
    	TreeSet<Conversation> conversations = new TreeSet<Conversation>();    	
		Conversation c;
    	int i = 0;
    	       	
    	for (Entry<Bytes, List<Column>> e : mapConvs.entrySet())
    	{
    		c = new Conversation(e.getKey().toUTF8());
    		    		
        	c.setLastReceivedDate(Long.valueOf(Selector.getColumnTimestamp(listConvs, Bytes.fromByteArray(listConvs.get(i).getName()))));
        	setMessages = new TreeSet<Message>();        	
        	
    		for (Column colConv : e.getValue())
    		{
    			Message m = MessageUtil.fromJson(Selector.getColumnStringValue(colConv));
    			    			
        		setMessages.add(m);
        	}
        	        	
        	c.setMessages(setMessages);
        	conversations.add(c);        
        	++i;
    	}
    	
    	return conversations;
	}

	public static Mutator _addConvToFolder(Mutator mutator, Folder f, Conversation conv)
	{
    	return mutator.writeColumn(FolderUtil.FOLDERS, FolderUtil.getId(f), mutator.newColumn(Bytes.fromUuid(UuidHelper.newTimeUuid()), conv.getId()));		
	}	
	
	public static String toJson(Folder folder)
	{
		return gson.toJson(folder);
	}

	public static Mutator _delConvFromFolder(Mutator mutator, Folder f, UUID uuid)
	{
    	return mutator.deleteColumn(FolderUtil.FOLDERS, FolderUtil.getId(f), Bytes.fromUuid(uuid));		
	}
}

class MessageUtil
{
	protected static Gson gson = new Gson();	
	public final static String SUBJECT = "subject";
	public final static String BODY = "body";
	public final static String SENDER = "sender";
	
	public static String toJson(Message m)
	{
		return gson.toJson(m);
	}
	
	public static Message fromJson(String strJson)
	{
		return gson.fromJson(strJson, Message.class);
	}

	public static String genId()
	{
		return UuidHelper.newTimeUuid().toString();
	}
}

class ConversationUtil
{
	public static final String CONVERSATIONS = "conversations";
	protected static String pool = "pool";
	
	public static boolean addMessageAndUpdateFolder(Folder f, Conversation conv, Message msg)
	{
		try
		{
        	Mutator mutator = PelopsUtil.getPool().createMutator();        		        		
        	// add message to conversation        	
        	_addMessageToConv(mutator, conv, msg);
        	        	
        	String strId[] = conv.getId().split(PostofficeUtil.DELIM); 
        	
        	// delete the conversation from the folder so we can move 
        	// it to the top
        	FolderUtil._delConvFromFolder(mutator, f, UUID.fromString(strId[1]));
        	
        	// move the conversation to the top of the folder 
        	// by re-adding it to update its column time stamp
        	FolderUtil._addConvToFolder(mutator, f, conv);
        	        
        	// run it
        	mutator.execute(ConsistencyLevel.ONE);
        	
        	return true;
		}
		catch (Exception e)
		{
			return false;
		}
	}
	
	public static boolean addMessageAndUpdateFolder(Folder f, String strId, Message msg)
	{
		Conversation c = new Conversation(strId);
		return addMessageAndUpdateFolder(f, c, msg);
	}

	public static Conversation withId(String strId)
	{

		return new Conversation(strId);
	}

	private static Mutator _addMessageToConv(Mutator mutator, Conversation conv, Message msg)
	{		
		return mutator.writeColumn(CONVERSATIONS, conv.getId(), mutator.newColumn(Bytes.fromUuid(UUID.fromString(msg.getId())), MessageUtil.toJson(msg)));		
	}

	public static Conversation get(String strId)
	{
		Conversation c;
		
    	Selector selector = PelopsUtil.getPool().createSelector();
    	List<Column> columns = selector.getColumnsFromRow(CONVERSATIONS, strId, false, ConsistencyLevel.ONE);
    	TreeSet<Message> setMessages = new TreeSet<Message>();
    	
    	for (Column col : columns)
    	{
    		Message m = MessageUtil.fromJson(Selector.getColumnStringValue(col));    		
    		setMessages.add(m);
    	}
    	
    	c = new Conversation(strId);
    	c.setMessages(setMessages);
    	    	
		return c;
	}
	
	public static String genId()
	{
		return UuidHelper.newTimeUuid().toString();
	}	
}

class PostofficeUtil
{
	public static final String DELIM = ":";

	public static void startConversation(String strFrom, String strTo, String strSubject, String strBody)
	{
		String strId = ConversationUtil.genId();		
		sendMessage(strFrom, strTo, strSubject, strBody, strId);
	}
	
	public static void sendMessage(String strFrom, String strTo, String strSubject, String strBody, String strId)
	{
		Message msg = new Message(
		  MessageUtil.genId(),
		  strFrom,
		  strSubject,
		  strBody
		);
				
		String strInboxId = strTo + DELIM + strId;
		String strSentId = strFrom + DELIM + strId;
		
		// create conversation in recipient's Inbox
		Conversation conv = new Conversation(strInboxId);
		Folder inbox = new Folder(strTo, "inbox");				
		ConversationUtil.addMessageAndUpdateFolder(inbox, conv, msg);
		
		// create conversation in sender's Sentbox
		conv = new Conversation(strSentId);
		Folder sent = new Folder(strFrom, "sent");				
		ConversationUtil.addMessageAndUpdateFolder(sent, conv, msg);
	}		
}

class PelopsUtil
{
	protected static IThriftPool pool;
	
	public static void connect()
	{
    	String keyspace = "hmb";
    	Cluster cluster = new Cluster("localhost", 9160);
    	pool = new CommonsBackedPool(cluster, keyspace);
	}
	
	public static void disconnect()
	{
		// shut down the pool
		pool.shutdown();
	}
	
	public static IThriftPool getPool()
	{
		return pool;
	}
}
	
interface HttpdHandler extends HttpHandler
{
	// hide away sun's HttpHandler so we can do more 
	// with it if we need to (abstract more)
}


class IndexHandler implements HttpdHandler
{
	@Override
	public void handle(HttpExchange e) throws IOException
	{	
		OutputStream outStream = e.getResponseBody();
		
		StringBuffer strBuf = new StringBuffer();
		strBuf.append("<html><head><title>Postoffice: Threaded messaging over HTTP & Cassandra</title></head><body>");
		strBuf.append("<h1>Welcome to Postoffice!</h1>");
		strBuf.append("</body></html>");
		
		e.getResponseHeaders().add("Content-type", "text/html");	
		e.sendResponseHeaders(HTTP_OK, strBuf.length());        
        outStream.write(strBuf.toString().getBytes());
        outStream.close();
        e.close();
	}
	
	public static IndexHandler get()
	{
		return new IndexHandler();
	}
}

class FolderHandler implements HttpdHandler
{
	protected final String COUNT = "count";
	protected final String OWNER = "owner";
	protected final String START = "start";
	protected final String FOLDER = "folder";
	
	@Override
	public void handle(HttpExchange e) throws IOException
	{
		QueryStringDecoder decoder = new QueryStringDecoder(e.getRequestURI());
		Map<String, List<String>> mapParams = decoder.getParameters();
	
		String strOwner = mapParams.get(OWNER).get(0);
		Long lStart = Long.parseLong(mapParams.get(START).get(0));
		Integer iCount = Integer.parseInt(mapParams.get(COUNT).get(0));
		String strFolder = mapParams.get(FOLDER).get(0);
		
    	Folder inbox = new Folder(strOwner, strFolder);    	    	
    	try
    	{
    		Set<Conversation> convs = FolderUtil.getConversations(inbox, lStart, iCount);
    		inbox.setConversations(convs);
    	} 
    	catch (Exception ex)
    	{
    		System.out.println(ex.getMessage());
    		ex.printStackTrace();
    	}
		
		OutputStream outStream = e.getResponseBody();
		
		StringBuffer strBuf = new StringBuffer();
		strBuf.append(FolderUtil.toJson(inbox));
		
		e.getResponseHeaders().add("Content-type", "text/javascript");	
		e.sendResponseHeaders(HTTP_OK, strBuf.length());        
        outStream.write(strBuf.toString().getBytes());
        outStream.close();
        e.close();
	}	
	
	public static FolderHandler get()
	{
		return new FolderHandler();
	}
}

abstract class ConversationHandler implements HttpdHandler
{
	protected final String FROM = "from";
	protected final String TO = "to";
	protected final String SUBJECT = "subject";
	protected final String BODY = "body";
	protected final String ID = "id";
}

class NewConversationHandler extends ConversationHandler
{
	protected final String FROM = "from";
	protected final String TO = "to";
	protected final String SUBJECT = "subject";
	protected final String BODY = "body";
	
	@Override
	public void handle(HttpExchange e) throws IOException
	{	
		QueryStringDecoder decoder = new QueryStringDecoder(e.getRequestURI());
		Map<String, List<String>> mapParams = decoder.getParameters();
	
		String strFrom = mapParams.get(FROM).get(0);
		String strTo = mapParams.get(TO).get(0);
		String strSubject = mapParams.get(SUBJECT).get(0);
		String strBody = mapParams.get(BODY).get(0);
		
		StringBuffer strBuf = new StringBuffer();
				
    	try
    	{
    		PostofficeUtil.startConversation(strFrom, strTo, strSubject, strBody);
    		strBuf.append("{\"status\":\"ok\"}");
    	} 
    	catch (Exception ex)
    	{
    		System.out.println(ex.getMessage());
    		ex.printStackTrace();
    		strBuf.append("{\"status\":\"err\"}");

    	}
		
		OutputStream outStream = e.getResponseBody();						
		e.getResponseHeaders().add("Content-type", "text/javascript");	
		e.sendResponseHeaders(HTTP_OK, strBuf.length());        
        outStream.write(strBuf.toString().getBytes());
        outStream.close();
        e.close();        
	}
	
	public static NewConversationHandler get()
	{
		return new NewConversationHandler();
	}
}

class ReplyHandler extends ConversationHandler
{
	@Override
	public void handle(HttpExchange e) throws IOException
	{		
		QueryStringDecoder decoder = new QueryStringDecoder(e.getRequestURI());
		Map<String, List<String>> mapParams = decoder.getParameters();
	
		String strFrom = mapParams.get(FROM).get(0);
		String strTo = mapParams.get(TO).get(0);
		String strSubject = mapParams.get(SUBJECT).get(0);
		String strBody = mapParams.get(BODY).get(0);
		String strId = mapParams.get(ID).get(0);
		
		StringBuffer strBuf = new StringBuffer();
				
    	try
    	{
    		PostofficeUtil.sendMessage(strFrom, strTo, strSubject, strBody, strId);
    		strBuf.append("{\"status\":\"ok\"}");
    	} 
    	catch (Exception ex)
    	{
    		System.out.println(ex.getMessage());
    		ex.printStackTrace();
    		strBuf.append("{\"status\":\"err\"}");
    	}
		
		OutputStream outStream = e.getResponseBody();						
		e.getResponseHeaders().add("Content-type", "text/javascript");	
		e.sendResponseHeaders(HTTP_OK, strBuf.length());        
        outStream.write(strBuf.toString().getBytes());
        outStream.close();
        e.close();        
	}
	
	public static ReplyHandler get()
	{
		return new ReplyHandler();
	}
}

class HttpServer
{
	protected Integer m_intPort;
	
	public HttpServer(Integer intPort)
	{
		m_intPort = intPort;
	}
	
	public void start()
	{
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new HttpServerPipelineFactory());

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(m_intPort));
	}
}

class HttpResponseHandler extends SimpleChannelUpstreamHandler
{

	private boolean readingChunks;

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		if(!readingChunks)
		{
			HttpResponse response = (HttpResponse) e.getMessage();

			System.out.println("STATUS: " + response.getStatus());
			System.out.println("VERSION: " + response.getProtocolVersion());
			System.out.println();

			if(!response.getHeaderNames().isEmpty())
			{
				for(String name : response.getHeaderNames())
				{
					for(String value : response.getHeaders(name))
					{
						System.out.println("HEADER: " + name + " = " + value);
					}
				}
				System.out.println();
			}

			if(response.isChunked())
			{
				readingChunks = true;
				System.out.println("CHUNKED CONTENT {");
			}
			else
			{
				ChannelBuffer content = response.getContent();
				if(content.readable())
				{
					System.out.println("CONTENT {");
					System.out.println(content.toString(CharsetUtil.UTF_8));
					System.out.println("} END OF CONTENT");
				}
			}
		}
		else
		{
			HttpChunk chunk = (HttpChunk) e.getMessage();
			if(chunk.isLast())
			{
				readingChunks = false;
				System.out.println("} END OF CHUNKED CONTENT");
			}
			else
			{
				System.out.print(chunk.getContent().toString(CharsetUtil.UTF_8));
				System.out.flush();
			}
		}
	}
}

class HttpServerPipelineFactory implements ChannelPipelineFactory
{
	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		// Create a default pipeline implementation.
		ChannelPipeline pipeline = pipeline();
		pipeline.addLast("log", new LoggingHandler(InternalLogLevel.INFO));

		// Uncomment the following line if you want HTTPS
		// SSLEngine engine =
		// SecureChatSslContextFactory.getServerContext().createSSLEngine();
		// engine.setUseClientMode(false);
		// pipeline.addLast("ssl", new SslHandler(engine));

		pipeline.addLast("decoder", new HttpRequestDecoder());
		// Uncomment the following line if you don't want to handle HttpChunks.
		// pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
		pipeline.addLast("encoder", new HttpResponseEncoder());
		// Remove the following line if you don't want automatic content
		// compression.
		pipeline.addLast("deflater", new HttpContentCompressor());
		pipeline.addLast("handler", new HttpRequestHandler());
		return pipeline;
	}
}

class HttpRequestHandler extends SimpleChannelUpstreamHandler
{

	private HttpRequest request;
	private boolean readingChunks;
	/** Buffer that stores the response content */
	private final StringBuilder buf = new StringBuilder();

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		if(!readingChunks)
		{
			HttpRequest request = this.request = (HttpRequest) e.getMessage();

			if(is100ContinueExpected(request))
			{
				send100Continue(e);
			}

			buf.setLength(0);
			buf.append("WELCOME TO THE WILD WILD WEB SERVER\r\n");
			buf.append("===================================\r\n");

			buf.append("VERSION: " + request.getProtocolVersion() + "\r\n");
			buf.append("HOSTNAME: " + getHost(request, "unknown") + "\r\n");
			buf.append("REQUEST_URI: " + request.getUri() + "\r\n\r\n");

			for(Map.Entry<String, String> h : request.getHeaders())
			{
				buf.append("HEADER: " + h.getKey() + " = " + h.getValue() + "\r\n");
			}
			buf.append("\r\n");

			QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
			Map<String, List<String>> params = queryStringDecoder.getParameters();
			if(!params.isEmpty())
			{
				for(Entry<String, List<String>> p : params.entrySet())
				{
					String key = p.getKey();
					List<String> vals = p.getValue();
					for(String val : vals)
					{
						buf.append("PARAM: " + key + " = " + val + "\r\n");
					}
				}
				buf.append("\r\n");
			}

			if(request.isChunked())
			{
				readingChunks = true;
			}
			else
			{
				ChannelBuffer content = request.getContent();
				if(content.readable())
				{
					buf.append("CONTENT: " + content.toString(CharsetUtil.UTF_8) + "\r\n");
				}
				writeResponse(e);
			}
		}
		else
		{
			HttpChunk chunk = (HttpChunk) e.getMessage();
			if(chunk.isLast())
			{
				readingChunks = false;
				buf.append("END OF CONTENT\r\n");

				HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
				if(!trailer.getHeaderNames().isEmpty())
				{
					buf.append("\r\n");
					for(String name : trailer.getHeaderNames())
					{
						for(String value : trailer.getHeaders(name))
						{
							buf.append("TRAILING HEADER: " + name + " = " + value + "\r\n");
						}
					}
					buf.append("\r\n");
				}

				writeResponse(e);
			}
			else
			{
				buf.append("CHUNK: " + chunk.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
			}
		}
	}

	private void writeResponse(MessageEvent e)
	{
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		response.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
		response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

		if(keepAlive)
		{
			// Add 'Content-Length' header only for a keep-alive connection.
			response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
		}

		// Encode the cookie.
		String cookieString = request.getHeader(COOKIE);
		if(cookieString != null)
		{
			CookieDecoder cookieDecoder = new CookieDecoder();
			Set<Cookie> cookies = cookieDecoder.decode(cookieString);
			if(!cookies.isEmpty())
			{
				// Reset the cookies if necessary.
				CookieEncoder cookieEncoder = new CookieEncoder(true);
				for(Cookie cookie : cookies)
				{
					cookieEncoder.addCookie(cookie);
				}
				response.addHeader(SET_COOKIE, cookieEncoder.encode());
			}
		}

		// Write the response.
		ChannelFuture future = e.getChannel().write(response);

		// Close the non-keep-alive connection after the write operation is
		// done.
		if(!keepAlive)
		{
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void send100Continue(MessageEvent e)
	{
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
		e.getChannel().write(response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		e.getCause().printStackTrace();
		e.getChannel().close();
	}
}

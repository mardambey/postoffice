package postoffice;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.LinkedHashMap;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SliceRange;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.Bytes;
import com.google.gson.Gson;

public class App
{	
	static class PelopsUtil
	{
		public static void connect()
		{
			String pool = "pool";
	    	String keyspace = "hmb";

	    	// init the connection pool
	    	Cluster cluster = new Cluster("localhost", 9160);
	    	Pelops.addPool(pool, cluster, keyspace);
		}
		
		public static void disconnect()
		{
			// shut down the pool
			Pelops.shutdown();
		}
	}
	
    public static void main( String[] args ) throws Exception
    {    
    	PelopsUtil.connect();
    	
    	Conversation c = new Conversation();
    	c.setId("1501571:2");
    	
    	Message m = new Message();
    	m.setId(System.currentTimeMillis() + "");
    	m.setSender("1501572");
    	m.setSubject("Welcome to Postoffice! - " + new Date());
    	m.setBody("Postoffice is a simple messaging system using Cassandra!");
    	
    	if (ConversationUtil.addMessage(c, m))
    	{
    		System.out.println("Saved conversation!");
    	}
    	else
    	{
    		System.out.println("Message could not be saved!");
    	}
    	
    	c = ConversationUtil.get("1501571:2");
    	    	
    	System.out.println("== Conversation: " + c.getId());
    	for (Message msg : c.getMessages())
    	{
    		System.out.println("    -- Sender: " + msg.getSender());
    		System.out.println("    -- Subject: " + msg.getSubject());
    		System.out.println("    -- Body: " + msg.getBody());
    	}
    	
    	Folder inbox = new Folder("1501571", "inbox");
    	FolderUtil.addConversation(inbox, c);
    	
    	Set<Conversation> convs = FolderUtil.getConversations(inbox, 0L, 10);
    	
    	System.out.println("Folder (1501571 - inbox):");
    	
    	for (Conversation conv : convs)
    	{
        	System.out.println("== Conversation: " + conv.getId());
        	for (Message msg : conv.getMessages())
        	{
        		System.out.println("    -- Sender: " + msg.getSender());
        		System.out.println("    -- Subject: " + msg.getSubject());
        		System.out.println("    -- Body: " + msg.getBody());
        	}
    	}
    	
    	PelopsUtil.disconnect();
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
		return c.getId().compareTo(m_strId);
	}
}

class Message implements Comparable<Message>
{
	protected String m_strId;
	protected String m_strSubject;
	protected String m_strBody;
	protected String m_strSender;
	
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
		this.m_strId = id;
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
		this.m_strSubject = subject;
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
		this.m_strBody = body;
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
		this.m_strSender = sender;
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
		this.m_strName = name;
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
		this.m_strOwner = owner;
	}		
}

class FolderUtil
{
	protected static String pool = "pool";
	protected static String colFamily = "folders";
	
	public static boolean addConversation(Folder f, Conversation c)
	{
		try
		{
        	Mutator mutator = Pelops.createMutator(pool);        		        		       
        	mutator.writeColumn(colFamily, getId(f), mutator.newColumn(c.getId(), ""));        		        	
        	mutator.execute(ConsistencyLevel.ONE);		  
        	
        	// TODO: must update folder sort order
        	
        	return true;
		}
		catch (Exception e)
		{
			return false;
		}
	}
	
	public static String getId(Folder f)
	{
		return f.getOwner() + ":" + f.getName();
	}
	
	public static Set<Conversation> getConversations(Folder f, Long lStart, Integer iCount)
	{
		Conversation c;
		
    	Selector selector = Pelops.createSelector(pool);
    	SlicePredicate pred = new SlicePredicate();
    	SliceRange sliceRange = new SliceRange();
    	sliceRange.setStart("".getBytes());
    	sliceRange.setFinish("".getBytes());
    	sliceRange.setCount(iCount);
    	
    	pred.setSlice_range(sliceRange);
    	
    	List<Column> listConvs = selector.getColumnsFromRow(colFamily, getId(f), pred, ConsistencyLevel.ONE);
    	List<Bytes> listConvIds = new ArrayList<Bytes>();
    	
    	for (Column col: listConvs)
    	{
    		listConvIds.add(Bytes.fromUTF8(Selector.getColumnStringName(col)));
    	}
    	
    	KeyRange keyRange = new KeyRange();
    	keyRange.setCount(iCount);
    	keyRange.setStart_key(Selector.getColumnStringValue(listConvs.get(0)).getBytes());
    	keyRange.setEnd_key(Selector.getColumnStringValue(listConvs.get(listConvs.size() - 1)).getBytes());
    	
    	// TODO: dont hardcode this
    	LinkedHashMap<String, List<Column>> mapConvs = selector.getColumnsFromRowsUtf8Keys("conversations", keyRange, false, ConsistencyLevel.ONE);
    	TreeSet<Message> setMessages = new TreeSet<Message>();
    	TreeSet<Conversation> conversations = new TreeSet<Conversation>();
    	
    	for (Entry<String, List<Column>> e : mapConvs.entrySet())
    	{
    		c = new Conversation();
        	c.setId(e.getKey());
        	
    		for (Column colConv : e.getValue())
    		{
    			Message m = MessageUtil.fromJson(Selector.getColumnStringValue(colConv));    	
        		setMessages.add(m);
        	}
        	        	
        	c.setMessages(setMessages);
        	conversations.add(c);        	
    	}
    	
    	return conversations;
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
}

class ConversationUtil
{
	protected static String pool = "pool";
	protected static String colFamily = "conversations";	        	        	    	    	
	
	public static boolean addMessage(Conversation conv, Message msg)
	{
		try
		{
        	Mutator mutator = Pelops.createMutator(pool);        		        		       
        	mutator.writeColumn(colFamily, conv.getId(), mutator.newColumn(msg.getId(), MessageUtil.toJson(msg)));        		        	
        	mutator.execute(ConsistencyLevel.ONE);		  
        	
        	// TODO: must update folder sort order and bring this conv to the top
        	
        	return true;
		}
		catch (Exception e)
		{
			return false;
		}
	}
	
	public static Conversation get(String strId)
	{
		Conversation c;
		
    	Selector selector = Pelops.createSelector(pool);
    	List<Column> columns = selector.getColumnsFromRow(colFamily, strId, false, ConsistencyLevel.ONE);
    	TreeSet<Message> setMessages = new TreeSet<Message>();
    	
    	for (Column col : columns)
    	{
    		Message m = MessageUtil.fromJson(Selector.getColumnStringValue(col));    		
    		setMessages.add(m);
    	}
    	
    	c = new Conversation();
    	c.setId(strId);
    	c.setMessages(setMessages);
    	    	
		return c;
	}		
}

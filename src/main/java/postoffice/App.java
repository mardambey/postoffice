package postoffice;

import java.util.List;
import java.util.TreeSet;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
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
    	c.setId("1501571:1");
    	
    	Message m = new Message();
    	m.setId(System.currentTimeMillis() + "");
    	m.setSender("1501572");
    	m.setSubject("Welcome to Postoffice!");
    	m.setBody("Postoffice is a simple messaging system using Cassandra!");
    	
    	if (ConversationUtil.addMessage(c, m))
    	{
    		System.out.println("Saved conversation!");
    	}
    	else
    	{
    		System.out.println("Message could not be saved!");
    	}
    	
    	c = ConversationUtil.getConversation("1501571:1");
    	    	
    	System.out.println("Conversation: " + c.getId());
    	for (Message msg : c.getMessages())
    	{
    		System.out.println("Sender: " + msg.getSender());
    		System.out.println("Subject: " + msg.getSubject());
    		System.out.println("Body: " + msg.getBody());
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
class Conversation
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
        	return true;
		}
		catch (Exception e)
		{
			return false;
		}
	}
	
	public static Conversation getConversation(String strId)
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

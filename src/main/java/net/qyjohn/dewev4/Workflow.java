package net.qyjohn.dewev4;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

import org.dom4j.*;
import org.dom4j.io.SAXReader;

import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;


import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


public class Workflow
{
	public ConcurrentHashMap<String, WorkflowJob> jobs;
	public String uuid, bucket, prefix, ackQueue;
	public SAXReader reader;
	public Document document;
	List<String> longJobs = new ArrayList<String>();
	final static Logger logger = Logger.getLogger(Workflow.class);
	public boolean localExec = false;
	public AmazonS3Client client;

	
	/**
	 *
	 * Constructor
	 *
	 */
	 
	public Workflow(String uuid, String bucket, String prefix, boolean localExec, String ackQueue)
	{
		this.uuid = uuid;
		this.localExec = localExec;
		this.ackQueue  = ackQueue;

		this.bucket = bucket;
		this.prefix = prefix;
		client = new AmazonS3Client();
		reader = new SAXReader();
		
		try
		{
			// Initialize the HashMap for workflow jobs
			checkLongJobs();
			jobs = new ConcurrentHashMap<String, WorkflowJob>();
			parseDocument();
			parseWorkflow();	
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
	}
	
	public boolean isEmpty()
	{
		return jobs.isEmpty();
	}
	
	
	
	/**
	 *
	 * Load long-running job names from long.xml.
	 *
	 */
         
	public void checkLongJobs() throws Exception 
	{
		// Check if long.xml exist
		if ( client.doesObjectExist(bucket, prefix+"/long.xml") )
		{
			S3Object s3Object= client.getObject(bucket, prefix+"/long.xml");
			byte[] byteArray = IOUtils.toByteArray(s3Object.getObjectContent());
			String jobsXML = new String(byteArray);
			Element jobs = DocumentHelper.parseText(jobsXML).getRootElement();

			for ( Iterator iter = jobs.elementIterator( "job" ); iter.hasNext(); ) 
			{
				Element job = (Element) iter.next();
				longJobs.add(job.attribute("name").getValue());
			}
			logger.info("Found long-running job definition long.xml with the following jobs:");
			for (String s : longJobs)
			{
				logger.info("\t" + s);
			}
		}
		else
		{
			logger.info("The workflow does not long-running job definition long.xml.");
		}

		if (localExec)
		{
			logger.info("Workflow scheduler enforces local execution. All jobs are treated as long-running jobs.");
		}

	}

	
	
	/**
	 *
	 * Parse the work flow from dag.xml.
	 *
	 */
         
	public void parseDocument() throws Exception 
	{
		String dagFile = "/tmp/" + uuid + ".xml";
		download(bucket, prefix+"/dag.xml", dagFile);
		document = reader.read(new File(dagFile));
	}
	
	/**
	 *
	 * Parse jobs and job dependencies
	 *
	 */
	 
	public void parseWorkflow()
	{
		List<Element> jobs = document.getRootElement().elements("job");
		List<Element> children = document.getRootElement().elements("child");

		for(Element job : jobs) 
		{
			prepareJob(job);
		}
		for(Element child : children) 
		{
			prepareChild(child);
		}
		jobs = null;
		children = null;
	}
	
	
	/**
	 *
	 * Parse the dependencies of a job
	 *
	 */
	 
	public void prepareChild(Element child)
	{
		String child_id = child.attribute("ref").getValue();
		List<Element> parents = child.elements("parent");
		
		for (Element parent: parents)
		{
			String parent_id = parent.attribute("ref").getValue();
			jobs.get(child_id).addParent(parent_id);
			jobs.get(parent_id).addChild(child_id);
		}
	}

	
	/**
	 *
	 * Parse a job, extract job name (command) and command line arguments
	 *
	 */
	 
	public void prepareJob(Element element)
	{
		String id = element.attribute("id").getValue();
		String name = element.attribute("name").getValue();
		String command = name;		
		
		// Compose the command to execute
		Element args = element.element("argument");
		Node node;
		Element e;
		StringTokenizer st;
		for ( int i = 0, size = args.nodeCount(); i < size; i++ )
		{
			node = args.node(i);
			if ( node instanceof Element ) 
			{
				e = (Element) node;
				command = command + " " + e.attribute("name").getValue();
			}
			else
			{
				st = new StringTokenizer(node.getText().trim());
				while (st.hasMoreTokens()) 
				{
					command = command + " " + st.nextToken();
				}
			}
		}
		
		// Binary, input files, output files
		String binFiles = name, inFiles = "", outFiles = "";
		for ( Iterator iter = element.elementIterator( "uses" ); iter.hasNext(); ) 
		{
			Element file = (Element) iter.next();
			if (file.attribute("link").getValue().equals("input"))
			{
				// This is an input file
				if (file.attribute("executable") != null)
				{
					if (file.attribute("executable").getValue().equals("true"))
					{
						binFiles = binFiles + " " + file.attribute("name").getValue();
					}
					else
					{
						inFiles = inFiles + " " + file.attribute("name").getValue();
					}							
				}
				else
				{
					inFiles = inFiles + " " + file.attribute("name").getValue();
				}							
			}
			else
			{
				outFiles = outFiles + " " + file.attribute("name").getValue();
			}
		}
		
		// XML representation
		String xml = createXML(uuid, bucket, prefix, id, name, command, binFiles.trim(), inFiles.trim(), outFiles.trim());
		
		// Create a WorkflowJob object
//		writeJobInfo(bucket, prefix, id, xml);
		WorkflowJob job = new WorkflowJob(uuid, bucket, prefix, id, name, xml);	

		job.setCommand(command);
		job.setLongJob(localExec);
		if (longJobs.contains(name))
		{
			job.setLongJob(true);
		}
		jobs.put(id, job);
	}
	
	public String createXML(String workflow, String bucket, String prefix, String id, String name, String command, String binFiles, String inFiles, String outFiles)
	{
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement( "root" );
		root.addAttribute("workflow", workflow);
		root.addAttribute("bucket",   bucket);
		root.addAttribute("prefix",   prefix);
		root.addAttribute("id",       id);		
		root.addAttribute("name",     name);		
		root.addAttribute("command",  command);		
		root.addAttribute("binFiles", binFiles);		
		root.addAttribute("inFiles",  inFiles);		
		root.addAttribute("outFiles", outFiles);		
		root.addAttribute("ackQueue", ackQueue);		
		
        return document.asXML();
      }
      
      public void writeJobInfo(String bucket, String prefix, String id, String xml)
      {
	      	String key  = prefix + "/jobs/" + id;

		  	try
			{
				logger.debug("Uploading job definition " + id);
				client.putObject(bucket, key, xml);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}

      }

	public void download(String bucket, String key, String filename) 
	{
		try
		{
			S3Object object = client.getObject(new GetObjectRequest(bucket, key));
			InputStream in = object.getObjectContent();
			OutputStream out = new FileOutputStream(filename);
			IOUtils.copy(in, out);
			in.close();
			out.close();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}




package net.qyjohn.dewev4;

import com.amazonaws.services.lambda.runtime.*;

public class LambdaWorker implements RequestHandler<String, String>
{

	@Override
	public String handleRequest(String input, Context context)
	{
		return "Hello World: " + input;
	}
}


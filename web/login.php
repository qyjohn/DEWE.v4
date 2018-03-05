<?php
// include the AWS client library in your code
require "aws.phar";
use Aws\CognitoIdentity\CognitoIdentityClient;
use Aws\Sts\StsClient;

// initialize a Cognito identity client using the factory
$identityClient = CognitoIdentityClient::factory(array(
    'region'  => 'us-east-1',
    'version' => 'latest'
));

// call the GetId API with the required parameters
$idResp = $identityClient->getId(array(
	'AccountId' => '137834070286',
	'IdentityPoolId' => 'us-east-1:669eff0a-bade-4677-bf5d-01fa3d0a0932'
	// If you are authenticating your users through an identity
	// provider then you can set the associative array of tokens
	// in this call
	// 'Logins' => array(
	//	'graph.facebook.com' => 'your facebook session token',
	//)
));

// retrieve the identity id from the response data structure
$identityId = $idResp["IdentityId"];

// TODO: At this point you should save this identifier so you won't
// have to make this call the next time a user connects


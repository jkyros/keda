// Code generated by smithy-go-codegen DO NOT EDIT.

package kinesis

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	internalauth "github.com/aws/aws-sdk-go-v2/internal/auth"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/ptr"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Disables server-side encryption for a specified stream. When invoking this API,
// it is recommended you use the StreamARN input parameter rather than the
// StreamName input parameter. Stopping encryption is an asynchronous operation.
// Upon receiving the request, Kinesis Data Streams returns immediately and sets
// the status of the stream to UPDATING . After the update is complete, Kinesis
// Data Streams sets the status of the stream back to ACTIVE . Stopping encryption
// normally takes a few seconds to complete, but it can take minutes. You can
// continue to read and write data to your stream while its status is UPDATING .
// Once the status of the stream is ACTIVE , records written to the stream are no
// longer encrypted by Kinesis Data Streams. API Limits: You can successfully
// disable server-side encryption 25 times in a rolling 24-hour period. Note: It
// can take up to 5 seconds after the stream is in an ACTIVE status before all
// records written to the stream are no longer subject to encryption. After you
// disabled encryption, you can verify that encryption is not applied by inspecting
// the API response from PutRecord or PutRecords .
func (c *Client) StopStreamEncryption(ctx context.Context, params *StopStreamEncryptionInput, optFns ...func(*Options)) (*StopStreamEncryptionOutput, error) {
	if params == nil {
		params = &StopStreamEncryptionInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "StopStreamEncryption", params, optFns, c.addOperationStopStreamEncryptionMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*StopStreamEncryptionOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type StopStreamEncryptionInput struct {

	// The encryption type. The only valid value is KMS .
	//
	// This member is required.
	EncryptionType types.EncryptionType

	// The GUID for the customer-managed Amazon Web Services KMS key to use for
	// encryption. This value can be a globally unique identifier, a fully specified
	// Amazon Resource Name (ARN) to either an alias or a key, or an alias name
	// prefixed by "alias/".You can also use a master key owned by Kinesis Data Streams
	// by specifying the alias aws/kinesis .
	//   - Key ARN example:
	//   arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
	//   - Alias ARN example: arn:aws:kms:us-east-1:123456789012:alias/MyAliasName
	//   - Globally unique key ID example: 12345678-1234-1234-1234-123456789012
	//   - Alias name example: alias/MyAliasName
	//   - Master key owned by Kinesis Data Streams: alias/aws/kinesis
	//
	// This member is required.
	KeyId *string

	// The ARN of the stream.
	StreamARN *string

	// The name of the stream on which to stop encrypting records.
	StreamName *string

	noSmithyDocumentSerde
}

type StopStreamEncryptionOutput struct {
	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationStopStreamEncryptionMiddlewares(stack *middleware.Stack, options Options) (err error) {
	err = stack.Serialize.Add(&awsAwsjson11_serializeOpStopStreamEncryption{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsAwsjson11_deserializeOpStopStreamEncryption{}, middleware.After)
	if err != nil {
		return err
	}
	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = awsmiddleware.AddClientRequestIDMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddComputeContentLengthMiddleware(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = v4.AddComputePayloadSHA256Middleware(stack); err != nil {
		return err
	}
	if err = addRetryMiddlewares(stack, options); err != nil {
		return err
	}
	if err = addHTTPSignerV4Middleware(stack, options); err != nil {
		return err
	}
	if err = awsmiddleware.AddRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = awsmiddleware.AddRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addStopStreamEncryptionResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addOpStopStreamEncryptionValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opStopStreamEncryption(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = awsmiddleware.AddRecursionDetection(stack); err != nil {
		return err
	}
	if err = addRequestIDRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addendpointDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	return nil
}

func newServiceMetadataMiddleware_opStopStreamEncryption(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		SigningName:   "kinesis",
		OperationName: "StopStreamEncryption",
	}
}

type opStopStreamEncryptionResolveEndpointMiddleware struct {
	EndpointResolver EndpointResolverV2
	BuiltInResolver  builtInParameterResolver
}

func (*opStopStreamEncryptionResolveEndpointMiddleware) ID() string {
	return "ResolveEndpointV2"
}

func (m *opStopStreamEncryptionResolveEndpointMiddleware) HandleSerialize(ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler) (
	out middleware.SerializeOutput, metadata middleware.Metadata, err error,
) {
	if awsmiddleware.GetRequiresLegacyEndpoints(ctx) {
		return next.HandleSerialize(ctx, in)
	}

	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return out, metadata, fmt.Errorf("unknown transport type %T", in.Request)
	}

	input, ok := in.Parameters.(*StopStreamEncryptionInput)
	if !ok {
		return out, metadata, fmt.Errorf("unknown transport type %T", in.Request)
	}

	if m.EndpointResolver == nil {
		return out, metadata, fmt.Errorf("expected endpoint resolver to not be nil")
	}

	params := EndpointParameters{}

	m.BuiltInResolver.ResolveBuiltIns(&params)

	params.StreamARN = input.StreamARN

	params.OperationType = ptr.String("control")

	var resolvedEndpoint smithyendpoints.Endpoint
	resolvedEndpoint, err = m.EndpointResolver.ResolveEndpoint(ctx, params)
	if err != nil {
		return out, metadata, fmt.Errorf("failed to resolve service endpoint, %w", err)
	}

	req.URL = &resolvedEndpoint.URI

	for k := range resolvedEndpoint.Headers {
		req.Header.Set(
			k,
			resolvedEndpoint.Headers.Get(k),
		)
	}

	authSchemes, err := internalauth.GetAuthenticationSchemes(&resolvedEndpoint.Properties)
	if err != nil {
		var nfe *internalauth.NoAuthenticationSchemesFoundError
		if errors.As(err, &nfe) {
			// if no auth scheme is found, default to sigv4
			signingName := "kinesis"
			signingRegion := m.BuiltInResolver.(*builtInResolver).Region
			ctx = awsmiddleware.SetSigningName(ctx, signingName)
			ctx = awsmiddleware.SetSigningRegion(ctx, signingRegion)

		}
		var ue *internalauth.UnSupportedAuthenticationSchemeSpecifiedError
		if errors.As(err, &ue) {
			return out, metadata, fmt.Errorf(
				"This operation requests signer version(s) %v but the client only supports %v",
				ue.UnsupportedSchemes,
				internalauth.SupportedSchemes,
			)
		}
	}

	for _, authScheme := range authSchemes {
		switch authScheme.(type) {
		case *internalauth.AuthenticationSchemeV4:
			v4Scheme, _ := authScheme.(*internalauth.AuthenticationSchemeV4)
			var signingName, signingRegion string
			if v4Scheme.SigningName == nil {
				signingName = "kinesis"
			} else {
				signingName = *v4Scheme.SigningName
			}
			if v4Scheme.SigningRegion == nil {
				signingRegion = m.BuiltInResolver.(*builtInResolver).Region
			} else {
				signingRegion = *v4Scheme.SigningRegion
			}
			if v4Scheme.DisableDoubleEncoding != nil {
				// The signer sets an equivalent value at client initialization time.
				// Setting this context value will cause the signer to extract it
				// and override the value set at client initialization time.
				ctx = internalauth.SetDisableDoubleEncoding(ctx, *v4Scheme.DisableDoubleEncoding)
			}
			ctx = awsmiddleware.SetSigningName(ctx, signingName)
			ctx = awsmiddleware.SetSigningRegion(ctx, signingRegion)
			break
		case *internalauth.AuthenticationSchemeV4A:
			v4aScheme, _ := authScheme.(*internalauth.AuthenticationSchemeV4A)
			if v4aScheme.SigningName == nil {
				v4aScheme.SigningName = aws.String("kinesis")
			}
			if v4aScheme.DisableDoubleEncoding != nil {
				// The signer sets an equivalent value at client initialization time.
				// Setting this context value will cause the signer to extract it
				// and override the value set at client initialization time.
				ctx = internalauth.SetDisableDoubleEncoding(ctx, *v4aScheme.DisableDoubleEncoding)
			}
			ctx = awsmiddleware.SetSigningName(ctx, *v4aScheme.SigningName)
			ctx = awsmiddleware.SetSigningRegion(ctx, v4aScheme.SigningRegionSet[0])
			break
		case *internalauth.AuthenticationSchemeNone:
			break
		}
	}

	return next.HandleSerialize(ctx, in)
}

func addStopStreamEncryptionResolveEndpointMiddleware(stack *middleware.Stack, options Options) error {
	return stack.Serialize.Insert(&opStopStreamEncryptionResolveEndpointMiddleware{
		EndpointResolver: options.EndpointResolverV2,
		BuiltInResolver: &builtInResolver{
			Region:       options.Region,
			UseDualStack: options.EndpointOptions.UseDualStackEndpoint,
			UseFIPS:      options.EndpointOptions.UseFIPSEndpoint,
			Endpoint:     options.BaseEndpoint,
		},
	}, "ResolveEndpoint", middleware.After)
}

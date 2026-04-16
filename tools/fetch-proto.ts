/**
 * Fetches the server's proto schema via gRPC server reflection.
 * Run this to diagnose proto mismatches:
 *
 *   npx ts-node tools/fetch-proto.ts
 *
 * Requires: SOLSTREAM_ENDPOINT in environment or .env
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import * as dotenv from 'dotenv';

dotenv.config({ path: path.resolve(__dirname, '../test-app/.env') });

const endpoint = (process.env.SOLSTREAM_ENDPOINT ?? '').replace(/^https?:\/\//, '');

// gRPC server reflection service proto (built into grpc-js)
const REFLECTION_PROTO = `
syntax = "proto3";
package grpc.reflection.v1alpha;
service ServerReflection {
  rpc ServerReflectionInfo(stream ServerReflectionRequest) returns (stream ServerReflectionResponse);
}
message ServerReflectionRequest {
  string host = 1;
  oneof message_request {
    string list_services = 4;
    string file_containing_symbol = 3;
    string file_by_filename = 1;
  }
}
message ServerReflectionResponse {
  string valid_host = 1;
  ServerReflectionRequest original_request = 2;
  oneof message_response {
    FileDescriptorResponse file_descriptor_response = 4;
    ListServiceResponse list_services_response = 6;
    ErrorResponse error_response = 7;
  }
}
message FileDescriptorResponse { repeated bytes file_descriptor_proto = 1; }
message ListServiceResponse { repeated ServiceResponse service = 1; }
message ServiceResponse { string name = 1; }
message ErrorResponse { int32 error_code = 1; string error_message = 2; }
`;

async function main() {
  const isSecure = (process.env.SOLSTREAM_ENDPOINT ?? '').startsWith('https');
  const creds = isSecure ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();

  // Write reflection proto to temp file
  const fs = await import('fs');
  const os = await import('os');
  const tmpProto = path.join(os.tmpdir(), 'reflection.proto');
  fs.writeFileSync(tmpProto, REFLECTION_PROTO);

  const pkg = protoLoader.loadSync(tmpProto, { keepCase: true });
  const proto = grpc.loadPackageDefinition(pkg) as any;
  const ReflectionService = proto.grpc.reflection.v1alpha.ServerReflection;

  const client = new ReflectionService(endpoint, creds);
  const call = client.ServerReflectionInfo();

  console.log(`Connecting to: ${endpoint}\n`);

  const results: string[] = [];

  call.on('data', (response: any) => {
    if (response.list_services_response) {
      const services = response.list_services_response.service.map((s: any) => s.name);
      console.log('Services found:');
      services.forEach((s: string) => console.log(' -', s));
      console.log('');
      results.push(...services);

      // Request file descriptors for each service
      for (const svc of services) {
        call.write({ file_containing_symbol: svc });
      }
      call.end();
    }

    if (response.file_descriptor_response) {
      const bufs: Buffer[] = response.file_descriptor_response.file_descriptor_proto;
      console.log(`Received ${bufs.length} file descriptor(s) — ${bufs.reduce((n, b) => n + b.length, 0)} bytes total`);
      console.log('');
      console.log('NOTE: Save these bytes and decode with protoc --decode_raw or');
      console.log('      use buf.build CLI to reconstruct .proto files.');
    }

    if (response.error_response) {
      console.error('Reflection error:', response.error_response.error_message);
    }
  });

  call.on('error', (err: Error) => {
    if (err.message.includes('UNIMPLEMENTED') || err.message.includes('unknown service')) {
      console.error('Server reflection is NOT enabled on this endpoint.');
      console.error('You must get the .proto files directly from the server operator.');
    } else {
      console.error('Error:', err.message);
    }
  });

  call.on('end', () => {
    console.log('Done.');
  });

  // Kick off with list_services
  call.write({ list_services: '' });
}

main().catch(console.error);

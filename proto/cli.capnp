@0xbdbbb6da8dc7b1e8;

struct CliRequest {
  args @0 :List(Text);
}

struct CliResponse {
  exitCode @0 :Int32;
  stdout @1 :Text;
  stderr @2 :Text;
}

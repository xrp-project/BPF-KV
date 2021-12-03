#include "client.h"

Client::Client(int id, ClientFactory *factory)
: id(id), factory(factory) {
	;
}

#include "workload.h"

Workload::Workload(long key_size, long value_size)
: key_size(key_size), value_size(value_size) {
	;
}

long Workload::generate_random_long(unsigned int *seedp) {
	return (((long)rand_r(seedp)) << (sizeof(int) * 8)) | rand_r(seedp);
}

double Workload::generate_random_double(unsigned int *seedp) {
	return ((double)rand_r(seedp)) / RAND_MAX;
}

UniformWorkload::UniformWorkload(long key_size, long value_size, long nr_entry,
			       long nr_op, double read_ratio, unsigned int seed)
: Workload(key_size, value_size), nr_entry(nr_entry), nr_op(nr_op), read_ratio(read_ratio), seed(seed), cur_nr_op(0) {
	sprintf(this->key_format, "%%0%ldld", key_size - 1);
}

bool UniformWorkload::has_next_op() {
	return this->cur_nr_op < this->nr_op;
}

void UniformWorkload::next_op(OperationType *type, char *key_buffer, char *value_buffer) {
	if (!this->has_next_op())
		throw std::invalid_argument("does not have next op");
	bool read = this->generate_random_double(&this->seed) <= this->read_ratio;
	if (read)
		*type = GET;
	else
		*type = SET;
	long key = this->generate_random_long(&this->seed) % this->nr_entry;
	this->generate_key_string(key_buffer, key);
	if (!read)
		this->generate_value_string(value_buffer);
	++this->cur_nr_op;
}

void UniformWorkload::generate_key_string(char *key_buffer, long key) {
	sprintf(key_buffer, this->key_format, key);
}

void UniformWorkload::generate_value_string(char *value_buffer) {
	for (int i = 0; i < this->value_size - 1; ++i) {
		value_buffer[i] = 'a' + (rand_r(&this->seed) % ('z' - 'a' + 1));
	}
	value_buffer[this->value_size - 1] = '\0';
}

ZipfianWorkload::ZipfianWorkload(long key_size, long value_size, long nr_entry, long nr_op, double read_ratio,
                                 double zipfian_constant, unsigned int seed)
: Workload(key_size, value_size), nr_entry(nr_entry), nr_op(nr_op), read_ratio(read_ratio),
  zipfian_constant(zipfian_constant), seed(seed) {
	sprintf(this->key_format, "%%0%ldld", key_size - 1);

	/* zipfian-related initialization */
	this->zetan = 0;
	for (long i = 1; i < this->nr_entry + 1; ++i) {
		this->zetan += 1.0 / (pow((double) i, this->zipfian_constant));
	}
	this->theta = this->zipfian_constant;
	this->zeta2theta = 0;
	for (long i = 1; i < 3; ++i) {
		this->zeta2theta += 1.0 / (pow((double) i, this->zipfian_constant));
	}
	this->alpha = 1.0 / (1.0 - this->theta);
	this->eta = (1 - pow(2.0 / (double) this->nr_entry, 1 - this->theta))
	            / (1 - (this->zeta2theta / this->zetan));
	this->generate_zipfian_random_ulong();
}

bool ZipfianWorkload::has_next_op() {
	return this->cur_nr_op < this->nr_op;
}

void ZipfianWorkload::next_op(OperationType *type, char *key_buffer, char *value_buffer) {
	if (!this->has_next_op())
		throw std::invalid_argument("does not have next op");
	bool read = this->generate_random_double(&this->seed) <= this->read_ratio;
	if (read)
		*type = GET;
	else
		*type = SET;
	long key = (long) (this->generate_zipfian_random_ulong() % ((unsigned long) this->nr_entry));
	this->generate_key_string(key_buffer, key);
	if (!read)
		this->generate_value_string(value_buffer);
	++this->cur_nr_op;
}

ZipfianWorkload * ZipfianWorkload::clone(unsigned int new_seed) {
	/* create a new ZipfianWorkload with a cheap nr_entry */
	ZipfianWorkload *copy = new ZipfianWorkload(this->key_size, this->value_size, 3, this->nr_op,
	                                            this->read_ratio, this->zipfian_constant, new_seed);
	copy->zetan = this->zetan;
	copy->theta = this->theta;
	copy->zeta2theta = this->zeta2theta;
	copy->alpha = this->alpha;
	copy->eta = this->eta;
	return copy;
}

unsigned long ZipfianWorkload::fnv1_64_hash(unsigned long value) {
	uint64_t hash = 14695981039346656037ul;
	uint8_t *p = (uint8_t *) &value;
	for (int i = 0; i < sizeof(unsigned long); ++i, ++p) {
		hash *= 1099511628211ul;
		hash ^= *p;
	}
	return (unsigned long) hash;
}

unsigned long ZipfianWorkload::generate_zipfian_random_ulong() {
	double u = this->generate_random_double(&this->seed);
	double uz = u * this->zetan;
	if (uz < 1)
		return 0;
	if (uz < 1 + pow(0.5, this->theta))
		return 1;
	unsigned long ret = (unsigned long) ((double)this->nr_entry * pow(this->eta * u - this->eta + 1, this->alpha));
	return ZipfianWorkload::fnv1_64_hash(ret);
}

void ZipfianWorkload::generate_key_string(char *key_buffer, long key) {
	sprintf(key_buffer, this->key_format, key);
}

void ZipfianWorkload::generate_value_string(char *value_buffer) {
	for (int i = 0; i < this->value_size - 1; ++i) {
		value_buffer[i] = 'a' + (rand_r(&this->seed) % ('z' - 'a' + 1));
	}
	value_buffer[this->value_size - 1] = '\0';
}

InitWorkload::InitWorkload(long nr_entry, long start_key, long key_size, long value_size, unsigned int seed)
: Workload(key_size, value_size), nr_entry(nr_entry), start_key(start_key), cur_nr_entry(0), seed(seed) {
	sprintf(this->key_format, "%%0%ldld", key_size - 1);
}

bool InitWorkload::has_next_op() {
	return this->cur_nr_entry < this->nr_entry;
}

void InitWorkload::next_op(OperationType *type, char *key_buffer, char *value_buffer) {
	if (!this->has_next_op())
		throw std::invalid_argument("does not have next op");
	*type = SET;
	this->generate_key_string(key_buffer, this->start_key + this->cur_nr_entry++);
	this->generate_value_string(value_buffer);
}

void InitWorkload::generate_key_string(char *key_buffer, long key) {
	sprintf(key_buffer, this->key_format, key);
}

void InitWorkload::generate_value_string(char *value_buffer) {
	for (int i = 0; i < this->value_size - 1; ++i) {
		value_buffer[i] = 'a' + (rand_r(&this->seed) % ('z' - 'a' + 1));
	}
	value_buffer[this->value_size - 1] = '\0';
}

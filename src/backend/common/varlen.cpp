/*-------------------------------------------------------------------------
 *
 * varlen.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/varlen.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/common/varlen.h"
#include "backend/common/pool.h"

namespace nstore {

Varlen* Varlen::Create(size_t size, Pool* dataPool){
	Varlen* retval;

	if (dataPool != NULL){
		retval = new(dataPool->Allocate(sizeof(Varlen))) Varlen(size, dataPool);
	}
	else{
		retval = new Varlen(size);
	}

	return retval;
}

void Varlen::Destroy(Varlen* varlen){
	delete varlen;
}

Varlen::Varlen(size_t size){
	varlen_size = size + sizeof(Varlen*);
	varlen_temp_pool = false;
	varlen_string_ptr = new char[varlen_size];
	SetBackPtr();
}

Varlen::Varlen(std::size_t size, Pool* dataPool){
	varlen_temp_pool = true;
	varlen_string_ptr = reinterpret_cast<char*>(dataPool->Allocate(size + sizeof(Varlen*)));
	SetBackPtr();
}

Varlen::~Varlen(){
	if (!varlen_temp_pool){
		delete[] varlen_string_ptr;
	}
}

char* Varlen::Get(){
	return varlen_string_ptr + sizeof(Varlen*);
}

const char* Varlen::Get() const{
	return varlen_string_ptr + sizeof(Varlen*);
}

void Varlen::UpdateStringLocation(void* location){
	varlen_string_ptr = reinterpret_cast<char*>(location);
}

void Varlen::SetBackPtr()
{
	Varlen** backptr = reinterpret_cast<Varlen**>(varlen_string_ptr);
	*backptr = this;
}

} // End nstore namespace

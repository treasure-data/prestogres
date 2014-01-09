#include "ruby.h"

void Init_prestogres_config()
{
    VALUE mPrestogres = rb_define_module("Prestogres");

	VALUE config = rb_hash_new();
	rb_hash_aset(config, rb_str_new2("prefix"), rb_str_new2(PRESTOGRES_PREFIX));

	rb_define_const(mPrestogres, "CONFIG", config);
}


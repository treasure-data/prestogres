#include "ruby.h"

void Init_presto_pggw_config()
{
    VALUE mPrestoPGGW = rb_define_module("PrestoPGGW");

	VALUE config = rb_hash_new();
	rb_hash_aset(config, rb_str_new2("prefix"), rb_str_new2(PRESTO_PGGW_PREFIX));

	rb_define_const(mPrestoPGGW, "CONFIG", config);
}


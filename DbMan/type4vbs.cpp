#include "type4vbs.h"

vbs_type_t type4vbs(MYSQL_FIELD *fd)
{
	switch (fd->type)
	{
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_LONGLONG:
	case MYSQL_TYPE_INT24:
	case MYSQL_TYPE_YEAR:
		return VBS_INTEGER;

	case MYSQL_TYPE_NULL:
		return VBS_NULL;

	case MYSQL_TYPE_FLOAT:
	case MYSQL_TYPE_DOUBLE:
		return VBS_FLOATING;

	case MYSQL_TYPE_DECIMAL:
	case MYSQL_TYPE_NEWDECIMAL:
		return VBS_DECIMAL;

	case MYSQL_TYPE_VARCHAR:
		return VBS_STRING;

	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
		return (fd->flags & BINARY_FLAG) ? VBS_BLOB : VBS_STRING;

	default:
		return VBS_STRING;
	}
	return VBS_STRING;
}


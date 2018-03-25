#include "SQLResult.h"
#include "xic/VData.h"
#include "xic/XicException.h"
#include "xslib/escape.h"

SQLResult::SQLResult(const xic::AnswerPtr& answer, int sql_no)
	: _answer(answer)
{
	if (!_answer)
		throw XERROR_MSG(XArgumentError, "NULL SQLResult");
	else if (_answer->status())
		throw XERROR_MSG(XArgumentError, "Exceptional Answer");

	const vbs_dict_t *dict = NULL;
	if (sql_no < 0)
	{
		dict = _answer->args_dict();
	}
	else
	{
		xic::AnswerReader ar(_answer);
		xic::VList results = ar.wantVList("results");
		if ((size_t)sql_no >= results.count())
		{
			throw XERROR_MSG(XArgumentError, "Invalid sql_no");
		}

		xic::VList::Node node = results.first();
		for (int i = 0; i < sql_no; ++i)
		{
			++node;
		}
		dict = node.dictValue();
	}

	xic::VDict vd(dict);
	_fields_list = vd.get_list("fields");
	_rows_list = vd.get_list("rows");
	_affected_rows = vd.getInt("affectedRowNumber");
	_insert_id = vd.getInt("insertId");

	_num_fields = _fields_list ? _fields_list->count : 0;
	_num_rows = _rows_list ? _rows_list->count : 0;
	_current_row = _rows_list ? _rows_list->first : NULL;

	_fields = NULL;
	_current_cols = NULL;
	_current_data_cols = NULL;
}

SQLResult::~SQLResult()
{
}

void SQLResult::reset()
{
	_current_row = _rows_list ? _rows_list->first : NULL;
}


xstr_t** SQLResult::fields()
{
	if (_fields_list)
	{
		if (!_fields)
		{
			_fields = (xstr_t**)ostk_alloc(_answer->ostk(), _num_fields * sizeof(*_fields));
			vbs_litem_t *ent = _fields_list->first;
			for (int i = 0; i < _num_fields; ++i)
			{
				if (ent->value.kind != VBS_STRING)
					throw XERROR_MSG(xic::ParameterTypeException, "The element in SQLResult row LIST is not STRING");

				_fields[i] = &ent->value.d_xstr;
				ent = ent->next;
			}
		}
	}

	return _fields; 
}

xstr_t** SQLResult::fetch_row()
{
	if (_current_row)
	{
		if (!_current_cols)
			_current_cols = (xstr_t**)ostk_alloc(_answer->ostk(), _num_fields * sizeof(xstr_t*));

		if (_current_row->value.kind != VBS_LIST)
			throw XERROR_MSG(xic::ParameterTypeException, "SQLResult row kind is not LIST");

		vbs_list_t *l = _current_row->value.d_list;
		_current_row = _current_row->next;

		if (l->count < (size_t)_num_fields)
			throw XERROR_MSG(xic::ParameterDataException, "Not enough elements in SQLResult row LIST");

		vbs_litem_t *ent = l->first;
		for (int i = 0; i < _num_fields; ++i)
		{
			if (ent->value.kind != VBS_STRING && ent->value.kind != VBS_BLOB)
				throw XERROR_MSG(xic::ParameterTypeException, "The element in SQLResult row LIST is not STRING or BLOB");

			_current_cols[i] = &ent->value.d_xstr;
			ent = ent->next;
		}
		return _current_cols;
	}
	return NULL;
}

vbs_data_t** SQLResult::fetch_row_data()
{
	if (_current_row)
	{
		if (!_current_data_cols)
			_current_data_cols = (vbs_data_t**)ostk_alloc(_answer->ostk(), _num_fields * sizeof(vbs_data_t*));

		if (_current_row->value.kind != VBS_LIST)
			throw XERROR_MSG(xic::ParameterTypeException, "SQLResult row kind is not LIST");

		vbs_list_t *l = _current_row->value.d_list;
		_current_row = _current_row->next;

		if (l->count < (size_t)_num_fields)
			throw XERROR_MSG(xic::ParameterDataException, "Not enough elements in SQLResult row LIST");

		vbs_litem_t *ent = l->first;
		for (int i = 0; i < _num_fields; ++i)
		{
			_current_data_cols[i] = &ent->value;
			ent = ent->next;
		}
		return _current_data_cols;
	}
	return NULL;
}

vbs_list_t* SQLResult::fetch_row_list()
{
	if (_current_row)
	{
		if (_current_row->value.kind != VBS_LIST)
			throw XERROR_MSG(xic::ParameterTypeException, "SQLResult row kind is not LIST");

		vbs_list_t *l = _current_row->value.d_list;
		_current_row = _current_row->next;

		if (l->count < (size_t)_num_fields)
			throw XERROR_MSG(xic::ParameterDataException, "Not enough elements in SQLResult row LIST");

		return l;
	}
	return NULL;
}

static xstr_t mysql_meta = XSTR_CONST("\r\n\x1a\x00'\"\\");

static int _escape(xio_write_function x_write, void *cookie, unsigned char ch)
{
	static xstr_t mysql_subst[] = 
	{
		XSTR_CONST("\\r"),
		XSTR_CONST("\\n"),
		XSTR_CONST("\\Z"),
		XSTR_CONST("\\0"),
	};

	unsigned char *p = (unsigned char *)memchr(mysql_meta.data, ch, mysql_meta.len);
	if (!p)
		return 0;

	int n = p - mysql_meta.data;
	if (n < (int)XS_ARRCOUNT(mysql_subst))
	{
		xstr_t& xs = mysql_subst[n];
		x_write(cookie, xs.data, xs.len);
	}
	else
	{
		char buf[2];
		buf[0] = '\\';
		buf[1] = ch;
		x_write(cookie, buf, 2);
	}

	return 1;
}

int mysql_xfmt(iobuf_t *ob, const xfmt_spec_t *spec, void *p)
{
	static bset_t mysql_bset = make_bset_from_xstr(&mysql_meta);
	int r = 0;
	
	if (xstr_equal_cstr(&spec->ext, "XSQL"))
	{
		escape_xstr((xio_write_function)iobuf_write, ob, &mysql_bset, _escape, (xstr_t *)p);
	}
	else if (xstr_equal_cstr(&spec->ext, "SQL"))
	{
		escape_cstr((xio_write_function)iobuf_write, ob, &mysql_bset, _escape, (char *)p);
	}       
	else
		r = -1;
	
	return r;
}


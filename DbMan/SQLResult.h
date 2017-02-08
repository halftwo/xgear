#ifndef SQLResult_h_
#define SQLResult_h_

#include "xic/XicMessage.h"


int mysql_xfmt(iobuf_t *ob, const xfmt_spec_t *spec, void *p);


class SQLResult
{
public:
	// For DBMan.sQuery(), sql_no = -1;
	// For DBMan.mQuery(), sql_no should be equal or greater than 0
	// and less than number of results (ie, number of sql statements)
	SQLResult(const xic::AnswerPtr& answer, int sql_no = -1);
	~SQLResult();

	size_t num_rows() const { return _num_rows; }
	size_t num_fields() const { return _num_fields; }
	size_t affected_rows() const { return _affected_rows; }
	intmax_t insert_id() const { return _insert_id; }

	xstr_t** fields();

	void reset();
	xstr_t** fetch_row();
	vbs_data_t** fetch_row_data();
	vbs_list_t* fetch_row_list();

private:
	xic::AnswerPtr _answer;
	const vbs_list_t *_fields_list;
	const vbs_list_t *_rows_list;
	int _num_fields;
	int _num_rows;
	size_t _affected_rows;
	intmax_t _insert_id;
	vbs_litem_t *_current_row;
	xstr_t** _fields;
	xstr_t** _current_cols;
	vbs_data_t** _current_data_cols;
};

#endif

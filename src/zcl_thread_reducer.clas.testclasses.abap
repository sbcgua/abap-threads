class ltct_test definition deferred.

class ltct_test_model definition
  inheriting from zcl_thread_reducer
  for testing
  duration short
  risk level harmless
  friends ltct_test.

  public section.

    types:
      begin of ty_payload,
        index type i,
        line  type string,
      end of ty_payload.

    data mt_payload type standard table of ty_payload.

    methods get_state_ref redefinition.
    methods create_runner redefinition.
    methods extract_result redefinition.

endclass.

class ltct_test_model implementation.

  method create_runner.
  endmethod.
  method extract_result.
  endmethod.

  method get_state_ref.
    get reference of mt_payload into rv_ref.
  endmethod.

endclass.

class ltct_test definition
  for testing
  duration short
  risk level harmless.
  private section.
    methods serialization for testing.
endclass.

class ltct_test implementation.

  method serialization.

    data lo type ref to ltct_test_model.
    data lv_xstr type xstring.
    data ls_state like lo->ms_state.
    data lt_payload like lo->mt_payload.
    field-symbols <p> like line of lo->mt_payload.

    create object lo.
    lo->set_run_params( iv_threads = 5 ).
    append initial line to lt_payload assigning <p>.
    <p>-index = 1.
    <p>-line  = 'Hello'.
    append initial line to lt_payload assigning <p>.
    <p>-index = 2.
    <p>-line  = 'World'.
    lo->mt_payload = lt_payload.

    cl_abap_unit_assert=>assert_equals(
      act = lo->ms_state-threads
      exp = 5 ).

    ls_state = lo->ms_state.

    lv_xstr = lo->serialize_state( ).
    clear lo->ms_state.
    clear lo->mt_payload.
    lo->deserialize_state( lv_xstr ).

    cl_abap_unit_assert=>assert_equals(
      act = lo->ms_state
      exp = ls_state ).

    cl_abap_unit_assert=>assert_equals(
      act = lo->mt_payload
      exp = lt_payload ).

  endmethod.

endclass.

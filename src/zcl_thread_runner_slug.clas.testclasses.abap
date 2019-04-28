class ltct_test_model definition
  inheriting from zcl_thread_runner_slug
  for testing
  duration short
  risk level harmless.

  public section.

    types:
      begin of ty_state,
        hello type string,
        world type string,
      end of ty_state.

    data ms_state type ty_state.

    methods get_state_ref redefinition.
    methods run redefinition.

endclass.

class ltct_test_model implementation.

  method run.
  endmethod.

  method get_state_ref.
    get reference of ms_state into rv_ref.
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
    data ls_temp like lo->ms_state.
    data lv_xstr type xstring.

    create object lo.
    lo->ms_state-hello = 'Hello'.
    lo->ms_state-world = 'World'.

    lv_xstr = lo->serialize_state( ).
    import data = ls_temp from data buffer lv_xstr.
    cl_abap_unit_assert=>assert_equals(
      act = ls_temp
      exp = lo->ms_state ).

    clear lo->ms_state.
    lo->deserialize_state( lv_xstr ).
    cl_abap_unit_assert=>assert_equals(
      act = lo->ms_state
      exp = ls_temp ).

  endmethod.

endclass.

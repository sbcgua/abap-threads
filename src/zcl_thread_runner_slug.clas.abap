class ZCL_THREAD_RUNNER_SLUG definition
  public
  abstract
  create public .

public section.

  constants C_RUNNER_FM_NAME type STRING value 'Z_THREAD_RUNNER'. "#EC NOTEXT

  methods RUN
  abstract .
  methods GET_STATE_REF
  abstract
    returning
      value(RV_REF) type ref to DATA .
  methods SERIALIZE_STATE
    returning
      value(RV_XSTR) type XSTRING .
  methods DESERIALIZE_STATE
    importing
      !IV_XSTR type XSTRING .
  methods RUN_PARALLEL
    importing
      !IV_THREAD_NAME type CLIKE optional .
  methods ON_END_OF_TASK
    importing
      !P_TASK type CLIKE .
  methods IS_READY
    returning
      value(RV_YES) type ABAP_BOOL .
  methods HAS_ERROR
    returning
      value(RV_YES) type ABAP_BOOL .
  methods ERROR
    returning
      value(RV_ERROR) type STRING .
  methods SET_DISPATCHER
    importing
      !IO_DISPATCHER type ref to ZCL_THREAD_QUEUE_DISPATCHER .
  protected section.

    data mv_error type string.
    data mv_ready type abap_bool.
    data mo_queue_dispatcher type ref to zcl_thread_queue_dispatcher.

  private section.
ENDCLASS.



CLASS ZCL_THREAD_RUNNER_SLUG IMPLEMENTATION.


  method deserialize_state.
    data lv_ref type ref to data.
    field-symbols <state> type any.

    lv_ref = get_state_ref( ).
    assign lv_ref->* to <state>.
    import data = <state> from data buffer iv_xstr.
    assert sy-subrc = 0.
  endmethod.


  method error.
    rv_error = mv_error.
  endmethod.


  method has_error.
    rv_yes = boolc( mv_error is not initial ).
  endmethod.


  method is_ready.
    rv_yes = mv_ready.
  endmethod.


  method on_end_of_task.

    data lv_xstr type xstring.

    receive results from function c_runner_fm_name
      importing
        ev_raw_result = lv_xstr
      exceptions
        others        = 4.

    if sy-subrc <> 0.
      mv_error = |{ sy-msgv1 }{ sy-msgv2 }{ sy-msgv3 }{ sy-msgv3 }|.
    else.
      deserialize_state( lv_xstr ).
    endif.

    mv_ready = abap_true.
    if mo_queue_dispatcher is bound. " Queued thread
      mo_queue_dispatcher->clear_thread( |{ p_task }| ).
    endif.

  endmethod.


  method run_parallel.

    data lv_class_name type string.
    data lv_xstr type xstring.
    data lv_thread_name type string.
    data lv_server_group type rzlli_apcl.

    lv_server_group = zcl_thread_queue_dispatcher=>c_default_group.
    lv_thread_name  = iv_thread_name.
    if mo_queue_dispatcher is bound. " Queued thread
      lv_server_group = mo_queue_dispatcher->get_server_group( ).
      lv_thread_name  = mo_queue_dispatcher->get_free_thread( ).
    endif.
    assert lv_thread_name is not initial.

    lv_class_name = cl_abap_typedescr=>describe_by_object_ref( me )->absolute_name.
    lv_xstr = serialize_state( ).

    call function c_runner_fm_name
      starting new task lv_thread_name
      destination in group lv_server_group
      calling on_end_of_task on end of task
      exporting
        iv_runner_class_name = lv_class_name
        iv_raw_params        = lv_xstr
      exceptions
        communication_failure = 1
        system_failure        = 2
        resource_failure      = 3
        others                = 4.

    if sy-subrc <> 0.
      mv_error = |{ sy-msgv1 }{ sy-msgv2 }{ sy-msgv3 }{ sy-msgv3 }|.
      mv_ready = abap_true.
    endif.

  endmethod.


  method serialize_state.
    data lv_ref type ref to data.
    field-symbols <state> type any.

    lv_ref = get_state_ref( ).
    assign lv_ref->* to <state>.
    export data = <state> to data buffer rv_xstr.
    assert sy-subrc = 0.
  endmethod.


  method set_dispatcher.
    mo_queue_dispatcher = io_dispatcher.
  endmethod.
ENDCLASS.

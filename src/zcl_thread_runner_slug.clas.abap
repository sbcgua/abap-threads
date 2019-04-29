class ZCL_THREAD_RUNNER_SLUG definition
  public
  abstract
  create public .

public section.

  constants c_runner_fm_name type string value 'Z_THREAD_RUNNER'. "#EC NOTEXT

  methods run
  abstract .
  methods get_state_ref
  abstract
    returning
      value(rv_ref) type ref to data .
  methods serialize_state
    returning
      value(rv_xstr) type xstring .
  methods deserialize_state
    importing
      !iv_xstr type xstring .
  methods run_parallel
    importing
      !iv_thread_name type clike optional .
  methods on_end_of_task
    importing
      !p_task type clike .
  methods is_ready
    returning
      value(rv_yes) type abap_bool .
  methods has_error
    returning
      value(rv_yes) type abap_bool .
  methods error
    returning
      value(rv_error) type string .
  methods set_dispatcher
    importing
      !io_dispatcher type ref to zcl_thread_queue_dispatcher .
  protected section.

    data mv_error type string.
    data mv_ready type abap_bool.
    data mo_queue_dispatcher type ref to zcl_thread_queue_dispatcher.

    methods debug
      importing
        iv_msg type string.

  private section.
ENDCLASS.



CLASS ZCL_THREAD_RUNNER_SLUG IMPLEMENTATION.


  method debug.
*    field-symbols <log> type string_table.
*    assign ('(ZTHREAD_RUNNER_TEST_MULTI_ONLY)gt_log') to <log>.
*    if sy-subrc is initial.
*      append iv_msg to <log>.
*    endif.
  endmethod.


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

    debug( |runner->run_parallel({ lv_thread_name }), pre start| ).

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

    data(rc) = sy-subrc.

    if rc = 3.
      data r_free_threads type i.
      CALL FUNCTION 'SPBT_GET_CURR_RESOURCE_INFO'
        IMPORTING
          free_pbt_wps                = r_free_threads
        EXCEPTIONS
          internal_error              = 1
          pbt_env_not_initialized_yet = 2
          OTHERS                      = 3.
      debug( |runner->run_parallel({ lv_thread_name }), resource_failure, free_threads = { r_free_threads }, rc={ sy-subrc }| ).
*      if r_free_threads = 0 or sy-subrc <> 0.
*        debug( '      ^^^^^^^^^^^^^^^^^^' ).
*      endif.
    endif.

    if rc <> 0.
      mv_error = |starting new task failed with rc={ rc }: { sy-msgv1 }{ sy-msgv2 }{ sy-msgv3 }{ sy-msgv3 }|.
      mv_ready = abap_true.
      debug( |runner->run_parallel({ lv_thread_name }), failed: { mv_error }| ).
      if mo_queue_dispatcher is bound. " Queued thread
        mo_queue_dispatcher->clear_thread( |{ lv_thread_name }| ).
      endif.
    else.
      debug( |runner->run_parallel({ lv_thread_name }), started ok| ).
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

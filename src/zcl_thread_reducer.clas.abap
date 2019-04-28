class ZCL_THREAD_REDUCER definition
  public
  inheriting from ZCL_THREAD_RUNNER_SLUG
  abstract
  create public .

public section.

  types:
    begin of ty_state,
        threads        type i,
        task_timeout   type i,
        reduce_timeout type i,
        task_prefix    type zcl_thread_queue_dispatcher=>ty_thread_name_prefix,
        server_group   type rzlli_apcl,
      end of ty_state .
  types:
    begin of ty_state_with_payload,
        state type ty_state,
        payload_buffer type xstring,
      end of ty_state_with_payload .
  types:
    begin of ty_queue,
        runner type ref to zcl_thread_runner_slug,
      end of ty_queue .

  constants C_DEFAULT_REDUCE_TIMEOUT type I value 300. "#EC NOTEXT

  methods SET_RUN_PARAMS   " Constructor MUST be without params
    importing
      !IV_THREADS type I
      !IV_TASK_TIMEOUT type I default ZCL_THREAD_QUEUE_DISPATCHER=>C_DEFAULT_TIMEOUT
      !IV_REDUCE_TIMEOUT type I default C_DEFAULT_REDUCE_TIMEOUT
      !IV_TASK_PREFIX type TY_STATE-TASK_PREFIX default ZCL_THREAD_QUEUE_DISPATCHER=>C_DEFAULT_TASK_PREFIX
      !IV_SERVER_GROUP type TY_STATE-SERVER_GROUP default ZCL_THREAD_QUEUE_DISPATCHER=>C_DEFAULT_GROUP .
  methods CREATE_RUNNER
  abstract
    importing
      !IO_QUEUE_DISPATCHER type ref to ZCL_THREAD_QUEUE_DISPATCHER
      !IV_INDEX type I
      !IV_PAYLOAD type DATA
    returning
      value(RO_INSTANCE) type ref to ZCL_THREAD_RUNNER_SLUG .
  methods EXTRACT_RESULT
  abstract
    importing
      !IO_RUNNER type ref to ZCL_THREAD_RUNNER_SLUG
      !IV_INDEX type I
    changing
      !CV_PAYLOAD type DATA .

  methods DESERIALIZE_STATE
    redefinition .
  methods RUN
    redefinition .
  methods SERIALIZE_STATE
    redefinition .
  protected section.

    data ms_state type ty_state. " TODO reconsider visibility ????

  private section.
ENDCLASS.



CLASS ZCL_THREAD_REDUCER IMPLEMENTATION.


  method deserialize_state.
    data ls_state_w_payload type ty_state_with_payload.
    data lv_ref type ref to data.
    field-symbols <ptr> type data.

    assign ls_state_w_payload to <ptr>.
    import data = <ptr> from data buffer iv_xstr.
    assert sy-subrc = 0.

    lv_ref = get_state_ref( ).
    assign lv_ref->* to <ptr>.
    import data = <ptr> from data buffer ls_state_w_payload-payload_buffer.
    assert sy-subrc = 0.
    ms_state = ls_state_w_payload-state.
  endmethod.


  method run.

    data lv_ref type ref to data.
    field-symbols <payload_tab> type any table.
    field-symbols <payload> type data.

    data lo_dispatcher type ref to zcl_thread_queue_dispatcher.
    create object lo_dispatcher
      exporting
        i_threads = ms_state-threads.

    data lt_queue type standard table of ty_queue.
    field-symbols <q> like line of lt_queue.

    lv_ref = get_state_ref( ).
    assign lv_ref->* to <payload_tab>.

    loop at <payload_tab> assigning <payload>.
      append initial line to lt_queue assigning <q>.
      <q>-runner = create_runner(
        io_queue_dispatcher = lo_dispatcher
        iv_index            = sy-tabix
        iv_payload          = <payload> ).
      <q>-runner->run_parallel( ).
    endloop.
    wait until lo_dispatcher->all_threads_are_finished( ) = abap_true up to 20 seconds.

    loop at <payload_tab> assigning <payload>.
      read table lt_queue assigning <q> index sy-tabix.
      assert sy-subrc = 0.
      extract_result(
        exporting
          io_runner = <q>-runner
          iv_index  = sy-tabix
        changing
          cv_payload = <payload> ).
    endloop.

  endmethod.


  method serialize_state.
    data ls_state_w_payload type ty_state_with_payload.
    data lv_ref type ref to data.
    field-symbols <ptr> type data.

    ls_state_w_payload-state = ms_state.
    lv_ref = get_state_ref( ).
    assign lv_ref->* to <ptr>.
    export data = <ptr> to data buffer ls_state_w_payload-payload_buffer.
    assert sy-subrc = 0.

    assign ls_state_w_payload to <ptr>.
    export data = <ptr> to data buffer rv_xstr.
    assert sy-subrc = 0.
  endmethod.


  method set_run_params.
    ms_state-threads        = iv_threads.
    ms_state-task_timeout   = iv_task_timeout.
    ms_state-reduce_timeout = iv_reduce_timeout.
    ms_state-task_prefix    = iv_task_prefix.
    ms_state-server_group   = iv_server_group.
  endmethod.
ENDCLASS.

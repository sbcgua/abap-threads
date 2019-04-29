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

  constants c_default_reduce_timeout type i value 300. "#EC NOTEXT

  methods set_run_params   " constructor must be without params
    importing
      !iv_threads type i
      !iv_task_timeout type i default zcl_thread_queue_dispatcher=>c_default_timeout
      !iv_reduce_timeout type i default c_default_reduce_timeout
      !iv_task_prefix type ty_state-task_prefix default zcl_thread_queue_dispatcher=>c_default_task_prefix
      !iv_server_group type ty_state-server_group default zcl_thread_queue_dispatcher=>c_default_group .
  methods create_runner
  abstract
    importing
      !io_queue_dispatcher type ref to zcl_thread_queue_dispatcher
      !iv_index type i
      !iv_payload type data
    returning
      value(ro_instance) type ref to zcl_thread_runner_slug .
  methods extract_result
  abstract
    importing
      !io_runner type ref to zcl_thread_runner_slug
      !iv_index type i
    changing
      !cv_payload type data .

  methods deserialize_state
    redefinition .
  methods run
    redefinition .
  methods serialize_state
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
        i_threads     = ms_state-threads
        i_task_prefix = ms_state-task_prefix
        i_group       = ms_state-server_group
        i_timeout     = ms_state-task_timeout.

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
    wait until lo_dispatcher->is_queue_empty( ) = abap_true up to 20 seconds.

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

program zthreads_test.

class lcx_error definition inheriting from cx_no_check.
endclass.

class lcl_task definition final inheriting from zcl_thread_runner_slug.
  public section.

    class-methods create " Constructor MUST be without params
      importing
        id            type i
        name          type string
        trigger_err   type abap_bool default abap_false
        io_dispatcher type ref to zcl_thread_queue_dispatcher optional
      returning
        value(ro_instance) type ref to lcl_task.

    methods result
      returning
        value(rv_result) type string.

    methods run redefinition.
    methods get_state_ref redefinition.

    types:
      begin of ty_state,
        id          type i,
        name        type string,
        trigger_err type abap_bool,
        result      type string,
      end of ty_state.

  private section.
    data ms_state type ty_state.

endclass.

class lcl_task implementation.

  method create.
    create object ro_instance.
    ro_instance->ms_state-id = id.
    ro_instance->ms_state-name = name.
    ro_instance->ms_state-trigger_err = trigger_err.
    ro_instance->set_dispatcher( io_dispatcher ).
  endmethod.

  method result.
    rv_result = ms_state-result.
  endmethod.

  method get_state_ref.
    get reference of ms_state into rv_ref.
  endmethod.

  method run.
    if ms_state-trigger_err = abap_true.
      raise exception type lcx_error.
    else.
      ms_state-result = |{ ms_state-id }: name = { ms_state-name }, time = { sy-uzeit }|.
    endif.
  endmethod.

endclass.

**********************************************************************

class zcl_thread_reducer definition abstract inheriting from zcl_thread_runner_slug.
  public section.

    constants c_default_reduce_timeout type i value 300.

    types:
      begin of ty_state,
        threads        type i,
        task_timeout   type i,
        reduce_timeout type i,
        task_prefix    type zcl_thread_queue_dispatcher=>ty_thread_name_prefix,
        server_group   type rzlli_apcl,
      end of ty_state.

    types:
      begin of ty_state_with_payload,
        state type ty_state,
        payload_buffer type xstring,
      end of ty_state_with_payload,
      begin of ty_queue,
        runner type ref to zcl_thread_runner_slug,
      end of ty_queue.

    methods set_run_params " Constructor MUST be without params
      importing
        iv_threads        type i
        iv_task_timeout   type i default zcl_thread_queue_dispatcher=>c_default_timeout
        iv_reduce_timeout type i default c_default_reduce_timeout
        iv_task_prefix    type ty_state-task_prefix default zcl_thread_queue_dispatcher=>c_default_task_prefix
        iv_server_group   type ty_state-server_group default zcl_thread_queue_dispatcher=>c_default_group.

    methods run redefinition.
    methods serialize_state redefinition.
    methods deserialize_state redefinition.

    methods create_runner abstract
      importing
        io_queue_dispatcher type ref to zcl_thread_queue_dispatcher
        iv_index            type i
        iv_payload          type data
      returning
        value(ro_instance) type ref to zcl_thread_runner_slug.

    methods extract_result abstract
      importing
        io_runner type ref to zcl_thread_runner_slug
        iv_index  type i
      changing
        cv_payload type data.

  protected section.
    data ms_state type ty_state. " TODO reconsider visibility ????

endclass.

class zcl_thread_reducer implementation.

  method set_run_params.
    ms_state-threads        = iv_threads.
    ms_state-task_timeout   = iv_task_timeout.
    ms_state-reduce_timeout = iv_reduce_timeout.
    ms_state-task_prefix    = iv_task_prefix.
    ms_state-server_group   = iv_server_group.
  endmethod.

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

endclass.

class lcl_reducer definition final inheriting from zcl_thread_reducer.
  public section.

    types:
      begin of ty_subject,
        payload type string,
        task    type string,
        error   type string,
        result  type string,
      end of ty_subject,
      tt_subject type standard table of ty_subject with key task.

    class-methods create " Constructor MUST be without params
      importing
        iv_threads  type i
        it_subjects type tt_subject
      returning
        value(ro_instance) type ref to lcl_reducer.

    methods result
      returning
        value(rt_result) type tt_subject.

    methods get_state_ref redefinition.
    methods create_runner redefinition.
    methods extract_result redefinition.

  private section.
    data mt_subjects type tt_subject.

endclass.

class lcl_reducer implementation.

  method create.
    create object ro_instance.
    ro_instance->set_run_params( iv_threads = iv_threads ).
    ro_instance->mt_subjects = it_subjects.
  endmethod.

  method get_state_ref.
    get reference of mt_subjects into rv_ref.
  endmethod.

  method result.
    rt_result = mt_subjects.
  endmethod.

  method create_runner.

    ro_instance = lcl_task=>create(
      io_dispatcher = io_queue_dispatcher
      id            = iv_index
      trigger_err   = boolc( iv_index = 4 ) "one random task failed
      name          = |Task { iv_index }| ).

  endmethod.

  method extract_result.

    data lo_runner type ref to lcl_task.
    field-symbols <res> like line of mt_subjects.

    assign cv_payload to <res>.
    lo_runner   ?= io_runner.
    <res>-task   = iv_index.
    <res>-result = lo_runner->result( ).
    <res>-error  = lo_runner->error( ).

  endmethod.

endclass.

**********************************************************************
* controller
**********************************************************************

class lcl_main definition final.
  public section.

    types:
      begin of ty_result,
        task type string,
        runner type ref to lcl_task,
      end of ty_result.

    methods constructor.
    methods run.

    data mo_dispatcher type ref to zcl_thread_queue_dispatcher.
    data mt_results type standard table of ty_result.

endclass.

class lcl_main implementation.

  method constructor.
    create object mo_dispatcher
      exporting
        i_threads = 2.
  endmethod.

  method run.

    field-symbols <res> like line of mt_results.
    data lv_msg type string.

    do 10 times.
      append initial line to mt_results assigning <res>.
      <res>-task = sy-tabix.
      <res>-runner = lcl_task=>create(
        io_dispatcher = mo_dispatcher
        id = sy-tabix
        trigger_err = boolc( sy-tabix = 3 ) "one random task failed
        name = |Task { sy-tabix }| ).
      <res>-runner->run_parallel( ).

    enddo.
    wait until mo_dispatcher->all_threads_are_finished( ) = abap_true up to 20 seconds.

    loop at mt_results assigning <res>.
      if <res>-runner->has_error( ) = abap_false.
        lv_msg = <res>-runner->result( ).
        write: / 'OK:', <res>-task, lv_msg.
      else.
        lv_msg = <res>-runner->error( ).
        write: / 'ER:', <res>-task, lv_msg.
      endif.
    endloop.

  endmethod.

endclass.

form run_single.

  uline.
  write: / 'Running same thread'.

  data lo_task type ref to lcl_task.
  lo_task = lcl_task=>create(
    id = 1
    name = 'Vasya' ).

  data lv_xstr type xstring.
  lv_xstr = lo_task->serialize_state( ).

  data lv_class_name type string.
  lv_class_name = cl_abap_typedescr=>describe_by_object_ref( lo_task )->absolute_name.

  call function zcl_thread_runner_slug=>c_runner_fm_name
    exporting
      iv_runner_class_name = lv_class_name
      iv_raw_params        = lv_xstr
    importing
      ev_raw_result        = lv_xstr
    exceptions
      other                = 4.

  if sy-subrc is not initial.
    data errmsg type c length 255.
    concatenate sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4 into errmsg.
    write: / 'Error:', errmsg.
  else.
    lo_task->deserialize_state( lv_xstr ).
    data lv_msg type string.
    lv_msg = lo_task->result( ).
    write: / lv_msg.
  endif.

endform.

form run_1_parallel.

  data lv_msg type string.
  data lo_task type ref to lcl_task.
  lo_task = lcl_task=>create(
    id = 1
*    trigger_err = abap_true
    name = 'Vasya' ).

  uline.
  write: / 'Running parallel'.
  lo_task->run_parallel( 'thread1' ).
  wait until lo_task->is_ready( ) = abap_true up to 10 seconds.

  if lo_task->error( ) is not initial.
    lv_msg = lo_task->error( ).
    write: / 'Error:', lv_msg.
  else.
    lv_msg = lo_task->result( ).
    write: / 'Result:', lv_msg.
  endif.

endform.

form run_with_reducer.

  " Payload
  data lt_subjects type lcl_reducer=>tt_subject.
  field-symbols <s> like line of lt_subjects.
  do 8 times.
    append initial line to lt_subjects assigning <s>.
    <s>-payload = sy-tabix.
  enddo.

  data lo_reducer type ref to lcl_reducer.
  lo_reducer = lcl_reducer=>create(
    iv_threads  = 2
    it_subjects = lt_subjects ).

  uline.
  write: / 'Running parallel with reducer'.
  lo_reducer->run_parallel( 'reducer' ).
  wait until lo_reducer->is_ready( ) = abap_true up to 10 seconds.

  data lt_results type lcl_reducer=>tt_subject.
  field-symbols <r> like line of lt_results.

  lt_results = lo_reducer->result( ).

  loop at lt_results assigning <r>.
    if <r>-error is not initial.
      write: / 'Error :', <r>-task, <r>-error.
    else.
      write: / 'Result:', <r>-task, <r>-result.
    endif.
  endloop.

endform.

form main.
  data lo_app type ref to lcl_main.
  create object lo_app.
  lo_app->run( ).

  perform run_1_parallel.
  perform run_single.
  perform run_with_reducer.

  uline.
  write: / 'Finished'.
endform.

start-of-selection.
  perform main.

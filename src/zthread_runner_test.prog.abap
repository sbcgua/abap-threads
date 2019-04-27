report zthreads_test.

class lcx_error definition inheriting from cx_no_check.
endclass.

class lcl_task definition final.
  public section.

    constants c_runner_fm_name type string value 'Z_THREAD_RUNNER'.

    interfaces zif_thread_runner_slug.
    aliases:
      run for zif_thread_runner_slug~run,
      serialize_params for zif_thread_runner_slug~serialize_params,
      deserialize_params for zif_thread_runner_slug~deserialize_params,
      serialize_result for zif_thread_runner_slug~serialize_result,
      deserialize_result for zif_thread_runner_slug~deserialize_result.

    methods init " Constructor MUST be without params
      importing
        id type i
        name type string
        trigger_err type abap_bool default abap_false.
    methods result
      returning
        value(rv_result) type string.
    methods run_parallel.
    methods is_ready
      returning
        value(rv_yes) type abap_bool.
    methods on_end_of_task
      importing
        p_task type clike.
    methods error
      returning
        value(rv_error) type string.


  private section.
    types:
      begin of ty_params,
        id          type i,
        name        type string,
        trigger_err type abap_bool,
      end of ty_params.
    data ms_params type ty_params.
    data mv_result type string.
    data mv_error type string.
    data mv_ready type abap_bool.
endclass.

class lcl_task implementation.

  method init.
    mv_ready = abap_false.
    ms_params-id = id.
    ms_params-name = name.
    ms_params-trigger_err = trigger_err.
  endmethod.

  method run_parallel.

    data lv_class_name type string.
    lv_class_name = cl_abap_typedescr=>describe_by_object_ref( me )->absolute_name.

    data lv_xstr type xstring.
    lv_xstr = serialize_params( ).

    call function c_runner_fm_name
      starting new task 'thread1' "lv_thread_name
      destination in group zcl_thread_handler=>c_default_group
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

  method on_end_of_task.

    data lv_xstr type xstring.

    receive results from function c_runner_fm_name
      importing
        ev_raw_result = lv_xstr
      exceptions
        others                = 4.

    if sy-subrc <> 0.
      mv_error = |{ sy-msgv1 }{ sy-msgv2 }{ sy-msgv3 }{ sy-msgv3 }|.
    else.
      deserialize_result( lv_xstr ).
    endif.

    mv_ready = abap_true.

  endmethod.

  method is_ready.
    rv_yes = mv_ready.
  endmethod.

  method result.
    rv_result = mv_result.
  endmethod.

  method error.
    rv_error = mv_error.
  endmethod.

  method run.
    if ms_params-trigger_err = abap_true.
      raise exception type lcx_error.
    else.
      mv_result = |{ ms_params-id }: name = { ms_params-name }, time = { sy-uzeit }|.
    endif.
  endmethod.
  method serialize_params.
    export data = ms_params to data buffer rv_xstr.
    assert sy-subrc = 0.
  endmethod.
  method deserialize_params.
    import data = ms_params from data buffer iv_xstr.
    assert sy-subrc = 0.
  endmethod.
  method serialize_result.
    export data = mv_result to data buffer rv_xstr.
    assert sy-subrc = 0.
  endmethod.
  method deserialize_result.
    import data = mv_result from data buffer iv_xstr.
    assert sy-subrc = 0.
  endmethod.

endclass.

class lcl_main definition final.
  public section.

    constants c_runner_fm_name type string value 'Z_THREAD_RUNNER'.

    types:
      begin of ty_result,
        task type string,
        result type string,
        error type string,
      end of ty_result.

    methods constructor.
    methods run.
    methods do_stuff_in_parallel importing i_id type i.
    methods on_end_of_action importing p_task type clike.
    methods report.

    data mo_handler type ref to zcl_thread_handler.
    data mt_results type standard table of ty_result.

endclass.

class lcl_main implementation.

  method constructor.
    create object mo_handler
      exporting
        i_threads = 2.
  endmethod.

  method report.
  endmethod.

  method run.

    field-symbols <res> like line of mt_results.

    do 10 times.
      me->do_stuff_in_parallel( sy-index ).
    enddo.
    wait until mo_handler->all_threads_are_finished( ) = abap_true up to 20 seconds.

    loop at mt_results assigning <res>.
      if <res>-result is not initial.
        write: / 'OK:', <res>-task, <res>-result.
      else.
        write: / 'ER:', <res>-task, <res>-error.
      endif.
    endloop.

  endmethod.

  method do_stuff_in_parallel.

    data lv_thread_name type char8.
    data errmsg type char255.

    lv_thread_name = mo_handler->get_free_thread( ).

    call function c_runner_fm_name
      starting new task lv_thread_name
      destination in group zcl_thread_handler=>c_default_group
      calling on_end_of_action on end of task
      exporting
        i_id = i_id
      exceptions
        communication_failure = 1 message errmsg
        system_failure        = 2 message errmsg
        resource_failure      = 3
        others                = 4.

     write: / 'Start Error:', sy-subrc, errmsg.

  endmethod.

  method on_end_of_action.

    data errmsg type c length 255.
    field-symbols <res> like line of mt_results.

    append initial line to mt_results assigning <res>.
    <res>-task = p_task.

    receive results from function c_runner_fm_name
      importing
        e_str = <res>-result
      exceptions
        communication_failure = 1 message errmsg
        system_failure        = 2 message errmsg
        others                = 4.

    if sy-subrc is not initial.
      if sy-subrc = 4.
        concatenate sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4 into errmsg.
      endif.
      <res>-error = errmsg.
    endif.

    mo_handler->clear_thread( |{ p_task }| ).

  endmethod.

endclass.

form run_single.

  data lo_task type ref to lcl_task.
  create object lo_task.
  lo_task->init(
    id = 1
    name = 'Vasya' ).

  data lv_xstr type xstring.
  lv_xstr = lo_task->serialize_params( ).

  data lv_class_name type string.
  lv_class_name = cl_abap_typedescr=>describe_by_object_ref( lo_task )->absolute_name.

  call function lcl_main=>c_runner_fm_name
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
    lo_task->deserialize_result( lv_xstr ).
    data lv_msg type string.
    lv_msg = lo_task->result( ).
    write: / lv_msg.
  endif.

endform.

form main.
*  data lo_app type ref to lcl_main.
*  create object lo_app.
*  lo_app->run( ).

  data lv_msg type string.
  data lo_task type ref to lcl_task.
  create object lo_task.
  lo_task->init(
    id = 1
*    trigger_err = abap_true
    name = 'Vasya' ).

  write: / 'Running parallel'.
  lo_task->run_parallel( ).
  wait until lo_task->is_ready( ) = abap_true up to 10 seconds.

  if lo_task->error( ) is not initial.
    lv_msg = lo_task->error( ).
    write: / 'Error:', lv_msg.
  else.
    lv_msg = lo_task->result( ).
    write: / 'Result:', lv_msg.
  endif.


*  perform run_single.
  write: / 'Finished'.
endform.

start-of-selection.
  perform main.

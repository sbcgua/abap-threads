report zthreads_test.

class lcx_error definition inheriting from cx_no_check.
endclass.

class lcl_task definition final inheriting from zcl_thread_runner_slug.
  public section.

    class-methods create " Constructor MUST be without params
      importing
        id          type i
        name        type string
        trigger_err type abap_bool default abap_false
        io_queue    type ref to zcl_thread_handler optional
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
    ro_instance->mo_queue_handler = io_queue.
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

    data mo_handler type ref to zcl_thread_handler.
    data mt_results type standard table of ty_result.

endclass.

class lcl_main implementation.

  method constructor.
    create object mo_handler
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
        io_queue = mo_handler
        id = 1
        trigger_err = boolc( sy-tabix = 3 ) "one random task failed
        name = |Task { sy-tabix }| ).
      <res>-runner->run_parallel( ).

    enddo.
    wait until mo_handler->all_threads_are_finished( ) = abap_true up to 20 seconds.

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

form main.
  data lo_app type ref to lcl_main.
  create object lo_app.
  lo_app->run( ).

  perform run_1_parallel.
  perform run_single.

  uline.
  write: / 'Finished'.
endform.

start-of-selection.
  perform main.

program zthread_runner_test.

class lcx_error definition inheriting from cx_no_check.
endclass.

class lcl_task definition final inheriting from zcl_thread_runner_slug.
  public section.

    class-methods create " Constructor MUST be without params
      importing
        id            type i
        name          type string
        trigger_err   type abap_bool default abap_false
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
      id            = iv_index
      trigger_err   = boolc( iv_index = 4 ) "one random task failed
      name          = |Task { iv_index }| ).
    ro_instance->set_dispatcher( io_queue_dispatcher ).

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

    methods run_single.
    methods run_1_parallel.
    methods run_multiple.
    methods run_with_reducer.

endclass.

class lcl_main implementation.

  method run_single.

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

  endmethod.

  method run_1_parallel.

    data lv_msg type string.
    data lo_task type ref to lcl_task.
    write: / 'Running parallel'.

    lo_task = lcl_task=>create(
      id = 1
*      trigger_err = abap_true
      name = 'Vasya' ).

    lo_task->run_parallel( 'thread1' ).
    wait until lo_task->is_ready( ) = abap_true up to 10 seconds.

    if lo_task->error( ) is not initial.
      lv_msg = lo_task->error( ).
      write: / 'Error:', lv_msg.
    else.
      lv_msg = lo_task->result( ).
      write: / 'Result:', lv_msg.
    endif.

  endmethod.

  method run_multiple.

    write: / 'Running multiple'.

    data lt_results type standard table of ty_result.
    data lv_msg type string.
    data lo_dispatcher type ref to zcl_thread_queue_dispatcher.
    field-symbols <res> like line of lt_results.

    create object lo_dispatcher
      exporting
        i_threads = 2.

    do 10 times.
      append initial line to lt_results assigning <res>.
      <res>-task = sy-tabix.
      <res>-runner = lcl_task=>create(
        id = sy-tabix
        trigger_err = boolc( sy-tabix = 3 ) "one random task failed
        name = |Task { sy-tabix }| ).
      <res>-runner->set_dispatcher( lo_dispatcher ).
      <res>-runner->run_parallel( ).

    enddo.
    wait until lo_dispatcher->all_threads_are_finished( ) = abap_true up to 20 seconds.

    loop at lt_results assigning <res>.
      if <res>-runner->has_error( ) = abap_false.
        lv_msg = <res>-runner->result( ).
        write: / 'OK:', <res>-task, lv_msg.
      else.
        lv_msg = <res>-runner->error( ).
        write: / 'ER:', <res>-task, lv_msg.
      endif.
    endloop.

  endmethod.

  method run_with_reducer.

    write: / 'Running parallel with reducer'.

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

  endmethod.

endclass.

form main.
  data lo_app type ref to lcl_main.
  create object lo_app.

  write: / 'Started'.
  uline.
  lo_app->run_single( ).
  uline.
  lo_app->run_1_parallel( ).
  uline.
  lo_app->run_multiple( ).
  uline.
  lo_app->run_with_reducer( ).
  uline.
  write: / 'Finished'.
endform.

start-of-selection.
  perform main.

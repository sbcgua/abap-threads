program zthread_runner_test_simple.

* This programs uses 7.4 abap syntax ! It is OK to leave it inactive, it is just an example

class lcl_task definition final inheriting from zcl_thread_runner_slug.
  public section.

    class-methods create                " Constructor MUST be without params
      importing
        number_to_process  type i       " Parameter to process in thread
      returning
        value(ro_instance) type ref to lcl_task.

    methods result
      returning
        value(rv_result) type string.

    methods run redefinition.
    methods get_state_ref redefinition.

    types:
      begin of ty_state,
        number_to_process type i,       " Parameter to work on
        result            type string,  " Result to return, both should be in one structure
      end of ty_state.

  private section.
    data ms_state type ty_state.        " Worker state, NECESSARY AND SUFFICIENT to run thread worker
                                        " A worker class MAY have other member but they will stay
                                        " just in the main thread so must be related to
                                        " pre/post processing not to the parallel job
endclass.

class lcl_task implementation.

  method create.
    create object ro_instance.
    ro_instance->ms_state-number_to_process = number_to_process.
  endmethod.

  method result.
    rv_result = ms_state-result.
  endmethod.

  method get_state_ref.
    rv_ref = ref #( ms_state ).
  endmethod.

  method run.
    ms_state-result = |Hello from worker { ms_state-number_to_process }, time = { sy-uzeit }|.
  endmethod.

endclass.

**********************************************************************

class lcl_reducer definition final inheriting from zcl_thread_reducer.
  public section.

    types:
      begin of ty_subject,
        number_to_process type i,
        task              type string, " Just for log
        error             type string,
        result            type string,
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

    field-symbols <data> like line of mt_subjects.
    assign iv_payload to <data>.
    ro_instance = lcl_task=>create( number_to_process = <data>-number_to_process ).
    ro_instance->set_dispatcher( io_queue_dispatcher ).

  endmethod.

  method extract_result.

    data(lo_runner) = cast lcl_task( io_runner ).

    field-symbols <data> like line of mt_subjects.
    assign cv_payload to <data>.
    <data>-task   = iv_index.
    <data>-result = lo_runner->result( ).
    <data>-error  = lo_runner->error( ).

  endmethod.

endclass.

**********************************************************************
* controller
**********************************************************************

class lcl_main definition final.
  public section.
    methods constructor.
    methods run_1_parallel.
    methods run_with_reducer.

endclass.

class lcl_main implementation.

  method constructor.
*    data(queue_tmp) = new zcl_thread_queue_dispatcher( i_threads = 2 ).
  endmethod.

  method run_1_parallel.

    write: / 'Running one task parallel'.

    data lo_task type ref to lcl_task.
    lo_task = lcl_task=>create( number_to_process = 1 ).
    lo_task->run_parallel( 'thread1' ).
    wait until lo_task->is_ready( ) = abap_true up to 10 seconds.

    if lo_task->has_error( ) = abap_true.
      write: / 'Error:' color = 6, lo_task->error( ).
    else.
      write: / 'Result:', lo_task->result( ).
    endif.

  endmethod.

  method run_with_reducer.

    write: / 'Running parallel with reducer'.

    data(lt_subjects) = value lcl_reducer=>tt_subject(    " Payload
      for i = 1 then i + 1 while i <= 8
      ( number_to_process = i ) ).

    data(lo_reducer) = lcl_reducer=>create(
      iv_threads  = 2
      it_subjects = lt_subjects ).

    lo_reducer->run_parallel( 'reducer' ).
    wait until lo_reducer->is_ready( ) = abap_true up to 10 seconds.

    loop at lo_reducer->result( ) assigning field-symbol(<r>).
      if <r>-error is not initial.
        write: / 'Error :' color 6, <r>-task, <r>-error.
      else.
        write: / 'Result:', <r>-task, <r>-result.
      endif.
    endloop.

  endmethod.

endclass.

form main.
  data(lo_app) = new lcl_main( ).

  write: / 'Started'.
  uline.
  lo_app->run_1_parallel( ).
  uline.
  lo_app->run_with_reducer( ).
  uline.
  write: / 'Finished'.
endform.

start-of-selection.
  perform main.

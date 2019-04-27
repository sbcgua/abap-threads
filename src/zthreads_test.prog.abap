report zthreads_test.

class lcl_main definition final.
  public section.

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

    call function 'Z_THREAD_TESTWORK'
      starting new task lv_thread_name
      destination in group zcl_thread_handler=>c_default_group
      calling on_end_of_action on end of task
      exporting
        i_id = i_id
      exceptions
        communication_failure = 1 message errmsg
        system_failure        = 2 message errmsg
        resource_failure      = 3.

     write: / 'Start Error:', sy-subrc.

  endmethod.

  method on_end_of_action.

    data errmsg type c length 255.
    data l_result type string.
    field-symbols <res> like line of mt_results.

    receive results from function 'Z_THREAD_TESTWORK'
      importing
        e_str = l_result
      exceptions
        communication_failure = 1 message errmsg
        system_failure        = 2 message errmsg
        OTHERS                = 4.

    append initial line to mt_results assigning <res>.
    <res>-task = p_task.

    if sy-subrc is not initial.
      <res>-error = errmsg.
*      write: / 'Finish Error:', sy-subrc.
*      write: / errmsg.
*      write: / sy-msgid, sy-msgno, sy-msgv1, sy-msgv2, sy-msgv3, sy-msgv4.
    else.
      <res>-result = l_result.
    endif.

    " Free the thread for the next thread to run
    mo_handler->clear_thread( |{ p_task }| ).

  endmethod.

endclass.


form main.
  data lo_app type ref to lcl_main.
  create object lo_app.
  lo_app->run( ).
  write: / 'Finished'.
endform.

start-of-selection.
  perform main.

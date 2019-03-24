report zthreads_test.

class lcl_main definition final.

  public section.
    methods constructor.
    methods run.
    methods do_stuff_in_parallel importing i_id type i.
    methods on_end_of_action importing p_task type clike.

    data mo_handler type ref to zcl_thread_handler.

endclass.

class lcl_main implementation.

  method constructor.
    create object mo_handler
      exporting
        i_threads = 2.
  endmethod.

  method run.

    do 2 times.
      me->do_stuff_in_parallel( sy-index ).
    enddo.
    wait until mo_handler->all_threads_are_finished( ) = abap_true.

  endmethod.

  method do_stuff_in_parallel.
    data lv_thread_name type char8.
    data errmsg type char255.

    lv_thread_name = mo_handler->get_free_thread( ).

    call function 'ZTHREAD_TESTWORK'
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

    receive results from function 'ZTHREAD_TESTWORK'
      importing
        e_str = l_result
      exceptions
        communication_failure = 1 message errmsg
        system_failure        = 2 message errmsg.

    if sy-subrc is not initial.
      write: / 'Finish Error:', sy-subrc.
    endif.

    " Free the thread for the next thread to run
    mo_handler->clear_thread( |{ p_task }| ).

    write: / p_task, l_result.

  endmethod.

endclass.


form main.
  data lo_app type ref to lcl_main.
  create object lo_app.
  lo_app->run( ).
endform.

start-of-selection.
  perform main.

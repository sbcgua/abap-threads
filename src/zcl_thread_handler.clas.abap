CLASS zcl_thread_handler DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPE-POOLS abap .

    CONSTANTS:
      c_default_group TYPE rzlli_apcl VALUE 'parallel_generators', "#EC NOTEXT
      c_task          TYPE char6 VALUE 'PARALL'.            "#EC NOTEXT

    METHODS:
      all_threads_are_finished
        RETURNING
          VALUE(r_empty) TYPE abap_bool,
      clear_thread
        IMPORTING
          !i_task TYPE char8,
      constructor
        IMPORTING
          !i_task_prefix TYPE char6 DEFAULT c_task
          !i_threads     TYPE i
          !i_group       TYPE rzlli_apcl DEFAULT c_default_group,
      handle_resource_failure,
      get_free_thread
        RETURNING
          VALUE(r_thread) TYPE char8 .

  PROTECTED SECTION.

  PRIVATE SECTION.

    TYPES:
      BEGIN OF ty_thread,
        thread TYPE char8,
        used   TYPE abap_bool,
      END OF ty_thread .

    DATA:
      task_prefix   TYPE char6,
      threads_list  TYPE STANDARD TABLE OF ty_thread WITH DEFAULT KEY,
      threads       TYPE i,
      used_threads  TYPE i,
      group         TYPE rzlli_apcl.

    METHODS get_free_threads
      RETURNING
        VALUE(r_free_threads) TYPE i .
ENDCLASS.



CLASS ZCL_THREAD_HANDLER IMPLEMENTATION.


  METHOD all_threads_are_finished.
    r_empty = boolc( used_threads = 0 ).
  ENDMETHOD.


  METHOD clear_thread.
    FIELD-SYMBOLS <thread> LIKE LINE OF me->threads_list.
    READ TABLE me->threads_list
      WITH KEY used = abap_true thread = i_task
      ASSIGNING <thread>.
    <thread>-used = abap_false.
    used_threads = used_threads - 1.
  ENDMETHOD.


  METHOD constructor.
    me->group = i_group.
    me->task_prefix = i_task_prefix.

    " No more than 100 threads
    IF i_threads > 100.
      me->threads = 100.
    ELSEIF i_threads < 0.
      me->threads = 1.
    ELSE.
      me->threads = i_threads.
    ENDIF.

    DATA free_threads TYPE i.
    free_threads = me->get_free_threads( ).

    " Ensure that no more than half of the free threads are used
    free_threads = free_threads / 2 + 1.
    IF free_threads < me->threads.
      me->threads = free_threads.
    ENDIF.

    " Initialise threads
    DATA threadn TYPE n LENGTH 2 VALUE '00'.
    FIELD-SYMBOLS <thread> LIKE LINE OF me->threads_list.
    DO me->threads TIMES.
      APPEND INITIAL LINE TO me->threads_list ASSIGNING <thread>.
      <thread>-thread = me->task_prefix && threadn.
      <thread>-used   = abap_false.
      ADD 1 TO threadn.
    ENDDO.
  ENDMETHOD.


  METHOD get_free_thread.
    " Wait for a free thread
    WAIT UNTIL me->used_threads < me->threads.

    " Get number of first free thread
    FIELD-SYMBOLS <thread> LIKE LINE OF me->threads_list.
    READ TABLE me->threads_list WITH KEY used = abap_false ASSIGNING <thread>.

    ADD 1 TO used_threads.
    <thread>-used = abap_true.
    r_thread = <thread>-thread.
  ENDMETHOD.


  METHOD get_free_threads.
    " Get number of free threads
    CALL FUNCTION 'SPBT_INITIALIZE'
      EXPORTING
        group_name                     = me->group
      IMPORTING
        free_pbt_wps                   = r_free_threads
      EXCEPTIONS
        invalid_group_name             = 1
        internal_error                 = 2
        pbt_env_already_initialized    = 3
        currently_no_resources_avail   = 4
        no_pbt_resources_found         = 5
        cant_init_different_pbt_groups = 6
        OTHERS                         = 7.

    CASE sy-subrc.
      WHEN 0. " Do nothing

      WHEN 3.
        " Already initialised - get current number of free threads
        CALL FUNCTION 'SPBT_GET_CURR_RESOURCE_INFO'
          IMPORTING
            free_pbt_wps                = r_free_threads
          EXCEPTIONS
            internal_error              = 1
            pbt_env_not_initialized_yet = 2
            OTHERS                      = 3.

        IF sy-subrc IS NOT INITIAL.
          " Something has gone seriously wrong, so end it here.
          MESSAGE ID sy-msgid TYPE 'X' NUMBER sy-msgno WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
        ENDIF.

      WHEN OTHERS.
        " Something has gone seriously wrong, so end it here.
        MESSAGE ID sy-msgid TYPE 'X' NUMBER sy-msgno WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.

    ENDCASE.
  ENDMETHOD.


  METHOD handle_resource_failure.
    DATA free_threads TYPE i.
    free_threads = me->get_free_threads( ).
    IF free_threads <= 1 AND me->threads > 1.
      me->threads = me->threads - 1.
    ENDIF.

    WAIT UP TO 5 SECONDS. " Long enough for the system to update
    WAIT UNTIL me->used_threads LT me->threads. " Now there's an available thread
  ENDMETHOD.
ENDCLASS.

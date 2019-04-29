class ZCL_THREAD_QUEUE_DISPATCHER definition
  public
  final
  create public .

public section.
  type-pools abap .

  types:
    ty_thread_name_prefix type c length 6 .
  types:
    ty_thread_name        type c length 8 .

  constants c_default_group type rzlli_apcl value 'parallel_generators'. "#EC NOTEXT
  constants c_default_task_prefix type ty_thread_name_prefix value 'PARALL'. "#EC NOTEXT
  constants c_default_timeout type i value 5. "#EC NOTEXT
  constants c_max_threads type i value 20. "#EC NOTEXT

  methods constructor
    importing
      !i_threads type i
      !i_timeout type i default c_default_timeout
      !i_task_prefix type ty_thread_name_prefix default c_default_task_prefix
      !i_group type rzlli_apcl default c_default_group .
  methods all_threads_are_finished
    returning
      value(r_empty) type abap_bool .
  methods clear_thread
    importing
      !i_task type ty_thread_name .
  methods handle_resource_failure .
  methods get_free_thread
    returning
      value(r_thread) type ty_thread_name .
  methods get_server_group
    returning
      value(r_server_group) type rzlli_apcl .

  protected section.

  private section.

    types:
      begin of ty_thread,
        thread type ty_thread_name,
        used   type abap_bool,
      end of ty_thread .

    data:
      task_prefix  type ty_thread_name_prefix,
      threads_list type standard table of ty_thread with default key,
      threads      type i,
      used_threads type i,
      timeout      type i,
      group        type rzlli_apcl.

    methods get_free_threads
      returning
        value(r_free_threads) type i .
    methods debug
      importing
        iv_msg type string.
ENDCLASS.



CLASS ZCL_THREAD_QUEUE_DISPATCHER IMPLEMENTATION.


  method ALL_THREADS_ARE_FINISHED.

    debug( |queue->all_threads_are_finished| ).
    r_empty = boolc( used_threads = 0 ).
    " potential race condition? add end_of_queue flag ?
  endmethod.


  method CLEAR_THREAD.

    debug( |queue->clear_thread({ i_task }), { me->used_threads }/{ me->threads }| ).

    field-symbols <thread> like line of me->threads_list.
    read table me->threads_list assigning <thread>
      with key
        thread = i_task
        used = abap_true.
    <thread>-used = abap_false.
    used_threads = used_threads - 1.
  endmethod.


  method CONSTRUCTOR.
    me->group       = i_group.
    me->task_prefix = i_task_prefix.
    me->timeout     = i_timeout.

    if i_threads > c_max_threads.
      me->threads = 20.
    elseif i_threads < 1.
      me->threads = 1.
    else.
      me->threads = i_threads.
    endif.

    data free_threads type i.
    free_threads = me->get_free_threads( ).

    " Ensure that no more than half of the free threads are used
    me->threads = nmin( val1 = me->threads val2 = free_threads div 2 + 1 ).

    debug( |queue->constructor, [want { i_threads }, free { free_threads }] ~> { me->threads }| ).

    " Initialise threads
    data thread_no type n length 2 value '00'.
    field-symbols <thread> like line of me->threads_list.
    do me->threads times.
      append initial line to me->threads_list assigning <thread>.
      <thread>-thread = me->task_prefix && thread_no.
      <thread>-used   = abap_false.
      add 1 to thread_no.
    enddo.
  endmethod.


  method debug.
*    field-symbols <log> type string_table.
*    assign ('(ZTHREAD_RUNNER_TEST_MULTI_ONLY)gt_log') to <log>.
*    if sy-subrc is initial.
*      append iv_msg to <log>.
*    endif.
  endmethod.


  method GET_FREE_THREAD.
    " Wait for a free thread

    debug( |queue->get_free_thread, { me->used_threads }/{ me->threads }| ).

    if me->used_threads = me->threads.
      wait until me->used_threads < me->threads up to me->timeout seconds.
      data(err) = sy-subrc.
      debug( |queue->get_free_thread, after wait, rc={ err }| ).
      if err = 8. "Timeout
        assert 1 = 0.
        "TODO ???
      endif.
    endif.

    " Get number of first free thread
    field-symbols <thread> like line of me->threads_list.
    read table me->threads_list with key used = abap_false assigning <thread>.

    <thread>-used = abap_true.
    r_thread = <thread>-thread.
    add 1 to used_threads.
  endmethod.


  method GET_FREE_THREADS.
    " Get number of free threads
    call function 'SPBT_INITIALIZE'
      exporting
        group_name                     = me->group
      importing
        free_pbt_wps                   = r_free_threads
      exceptions
        invalid_group_name             = 1
        internal_error                 = 2
        pbt_env_already_initialized    = 3
        currently_no_resources_avail   = 4
        no_pbt_resources_found         = 5
        cant_init_different_pbt_groups = 6
        others                         = 7.

    " TODO graceful handling of error
    case sy-subrc.
      when 0. " Do nothing

      when 3.
        " Already initialised - get current number of free threads
        call function 'SPBT_GET_CURR_RESOURCE_INFO'
          importing
            free_pbt_wps                = r_free_threads
          exceptions
            internal_error              = 1
            pbt_env_not_initialized_yet = 2
            others                      = 3.

        if sy-subrc is not initial.
          " Something has gone seriously wrong, so end it here.
          message id sy-msgid type 'X' number sy-msgno with sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
        endif.

      when others.
        " Something has gone seriously wrong, so end it here.
        message id sy-msgid type 'X' number sy-msgno with sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.

    endcase.
  endmethod.


  method GET_SERVER_GROUP.
    r_server_group = me->group.
  endmethod.


  method HANDLE_RESOURCE_FAILURE.
    data free_threads type i.
    free_threads = me->get_free_threads( ).
    if free_threads <= 1 and me->threads > 1.
      me->threads = me->threads - 1.
    endif.

    wait up to 1 seconds. " Long enough for the system to update
    wait until me->used_threads lt me->threads. " Now there's an available thread
  endmethod.
ENDCLASS.

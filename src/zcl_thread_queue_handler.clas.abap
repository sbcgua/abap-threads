class ZCL_THREAD_QUEUE_HANDLER definition
  public
  final
  create public .

public section.
  type-pools ABAP .

  types:
    ty_thread_name_prefix type c length 6 .
  types:
    ty_thread_name        type c length 8 .

  constants C_DEFAULT_GROUP type RZLLI_APCL value 'parallel_generators'. "#EC NOTEXT
  constants C_DEFAULT_TASK_PREFIX type TY_THREAD_NAME_PREFIX value 'PARALL'. "#EC NOTEXT
  constants C_DEFAULT_WAIT_TIME type I value 5. "#EC NOTEXT
  constants C_MAX_THREADS type I value 20. "#EC NOTEXT

  methods constructor
    importing
      !i_threads type i
      !i_wait_time type i default c_default_wait_time
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
      wait_time    type i,
      group        type rzlli_apcl.

    methods get_free_threads
      returning
        value(r_free_threads) type i .
ENDCLASS.



CLASS ZCL_THREAD_QUEUE_HANDLER IMPLEMENTATION.


  method ALL_THREADS_ARE_FINISHED.
    r_empty = boolc( used_threads = 0 ).
  endmethod.


  method CLEAR_THREAD.
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
    me->wait_time   = i_wait_time.

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
    free_threads = free_threads / 2 + 1.
    if free_threads < me->threads.
      me->threads = free_threads.
    endif.

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


  method GET_FREE_THREAD.
    " Wait for a free thread
    wait until me->used_threads < me->threads up to me->wait_time seconds.

    " Get number of first free thread
    field-symbols <thread> like line of me->threads_list.
    read table me->threads_list with key used = abap_false assigning <thread>.

    add 1 to used_threads.
    <thread>-used = abap_true.
    r_thread = <thread>-thread.
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
